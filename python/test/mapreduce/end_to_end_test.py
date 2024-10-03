#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.

"""Testing mapreduce functionality end to end."""



# Using opensource naming conventions, pylint: disable=g-bad-name

import datetime
import logging
import random
import string
import unittest

from google.appengine.ext import db
from google.appengine.ext import ndb

from mapreduce import context
from mapreduce import control
from mapreduce import handlers
from mapreduce import input_readers
from mapreduce import model
from mapreduce import output_writers
from mapreduce import parameters
from mapreduce import records
from mapreduce import test_support
from mapreduce.tools import gcs_file_seg_reader
from testlib import testutil

from google.cloud import exceptions, storage

_storage_client = storage.Client()

def random_string(length):
  """Generate a random string of given length."""
  return "".join(
      random.choice(string.ascii_letters + string.digits) for _ in range(length))


class FakeEntity(db.Model):
  """Test entity class."""
  int_property = db.IntegerProperty()
  dt = db.DateTimeProperty(default=datetime.datetime(2000, 1, 1))


class NdbFakeEntity(ndb.Model):
  """Test entity class for NDB."""


class FakeHandler:
  """Test handler which stores all processed entities keys.

  Properties:
    processed_entites: all processed entities.
  """

  processed_entites = []

  def __call__(self, entity):
    """Main handler process function.

    Args:
      entity: entity to process.
    """
    FakeHandler.processed_entites.append(entity)

  @staticmethod
  def reset():
    """Clear processed_entites & reset delay to 0."""
    FakeHandler.processed_entites = []


class SerializableHandler:
  """Handler that utilize serialization."""

  _next_instance_id = 0
  # The first few instances will keep raising errors.
  # This is to test that upon shard retry, shard creates a new handler.
  INSTANCES_THAT_RAISE_ERRORS = 3
  # The first instance is created for validation and not used by any shard.
  FAILURES_INDUCED_BY_INSTANCE = INSTANCES_THAT_RAISE_ERRORS - 1

  def __init__(self):
    self.count = 0
    self.instance = self.__class__._next_instance_id
    self.__class__._next_instance_id += 1

  def __call__(self, entity):
    if self.instance < self.INSTANCES_THAT_RAISE_ERRORS:
      raise exceptions.GoogleCloudError("Injected error.")
    # Increment the int property by one on every call.
    entity.int_property = self.count
    entity.put()
    self.count += 1

  @classmethod
  def reset(cls):
    cls._next_instance_id = 0


def fake_handler_yield_key(entity):
  """Test handler which yields entity key."""
  yield entity.key()


def fake_handler_yield_ndb_key(entity):
  """Test handler which yields entity key (NDB version)."""
  yield entity.key


class FakeOutputWriter(output_writers.OutputWriter):
  """Test output writer."""

  file_contents = {}

  def __init__(self, filename):
    self.filename = filename

  @classmethod
  def reset(cls):
    cls.file_contents = {}

  @classmethod
  def validate(cls, mapper_spec):
    pass

  @classmethod
  def finalize_job(cls, mapreduce_state):
    pass

  @classmethod
  def create(cls, mr_spec, shard_number, shard_attempt, _writer_state=None):
    random_str = "".join(
        random.choice(string.ascii_uppercase + string.digits)
        for _ in range(64))
    cls.file_contents[random_str] = []
    return cls(random_str)

  def to_json(self):
    return {"filename": self.filename}

  @classmethod
  def from_json(cls, json_dict):
    return cls(json_dict["filename"])

  def write(self, data):
    self.file_contents[self.filename].append(data)

  def finalize(self, ctx, shard_number):
    pass


class EndToEndTest(testutil.HandlerTestBase):
  """Test mapreduce functionality end to end."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    FakeHandler.reset()
    FakeOutputWriter.reset()
    self.original_slice_duration = parameters.config._SLICE_DURATION_SEC
    SerializableHandler.reset()

  def tearDown(self):
    parameters.config._SLICE_DURATION_SEC = self.original_slice_duration

  def testHandlerSerialization(self):
    """Test serializable handler works with MR and shard retry."""
    entity_count = 10

    for _ in range(entity_count):
      FakeEntity(int_property=-1).put()

    # Force handler to serialize on every call.
    parameters.config._SLICE_DURATION_SEC = 0

    control.start_map(
        "fake_map",
        __name__ + ".SerializableHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
        },
        shard_count=1,
    )

    task_run_counts = test_support.execute_until_empty(self.taskqueue)
    self.assertEqual(
        task_run_counts[handlers.MapperWorkerCallbackHandler],
        # Shard retries + one per entity + one to exhaust input reader + one for
        # finalization.
        SerializableHandler.FAILURES_INDUCED_BY_INSTANCE + entity_count + 1 + 1)
    vals = [e.int_property for e in FakeEntity.all()]
    vals.sort()
    # SerializableHandler updates int_property to be incremental from 0 to 9.
    self.assertEqual(list(range(10)), vals)

  def testLotsOfEntities(self):
    entity_count = 1000

    for _ in range(entity_count):
      FakeEntity().put()

    control.start_map(
        "fake_map",
        __name__ + ".FakeHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
        },
        shard_count=4,
       )

    test_support.execute_until_empty(self.taskqueue)
    self.assertEqual(entity_count, len(FakeHandler.processed_entites))

  def testEntityQuery(self):
    entity_count = 1000

    for i in range(entity_count):
      FakeEntity(int_property=i % 5).put()

    control.start_map(
        "fake_map",
        __name__ + ".FakeHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
            "filters": [("int_property", "=", 3),
                        # Test datetime can be json serialized.
                        ("dt", "=", datetime.datetime(2000, 1, 1))],
        },
        shard_count=4,
      )

    test_support.execute_until_empty(self.taskqueue)
    self.assertEqual(200, len(FakeHandler.processed_entites))

  def testLotsOfNdbEntities(self):
    entity_count = 1000

    for _ in range(entity_count):
      NdbFakeEntity().put()

    control.start_map(
        "fake_map",
        __name__ + ".FakeHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + NdbFakeEntity.__name__,
        },
        shard_count=4,
      )

    test_support.execute_until_empty(self.taskqueue)
    self.assertEqual(entity_count, len(FakeHandler.processed_entites))

  def testInputReaderDedicatedParameters(self):
    entity_count = 100

    for _ in range(entity_count):
      FakeEntity().put()

    control.start_map(
        "fake_map",
        __name__ + ".FakeHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "input_reader": {
                "entity_kind": __name__ + "." + FakeEntity.__name__,
            },
        },
        shard_count=4,
      )

    test_support.execute_until_empty(self.taskqueue)
    self.assertEqual(entity_count, len(FakeHandler.processed_entites))

  def testOutputWriter(self):
    """End-to-end test with output writer."""
    entity_count = 1000

    for _ in range(entity_count):
      FakeEntity().put()

    control.start_map(
        "fake_map",
        __name__ + ".fake_handler_yield_key",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
        },
        shard_count=4,
        output_writer_spec=__name__ + ".FakeOutputWriter")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEqual(entity_count,
                      sum(map(len, list(FakeOutputWriter.file_contents.values()))))

  def testRecordsReader(self):
    """End-to-end test for records reader."""
    input_data = [str(i).encode() for i in range(100)]

    bucket_name = "byates"
    test_filename = "testfile"

    bucket = _storage_client.get_bucket(bucket_name)
    blob = bucket.blob(test_filename)

    with blob.open("wb") as f:
      with records.RecordsWriter(f) as w:
        for record in input_data:
          w.write(record)

    control.start_map(
        "fake_map",
        __name__ + ".FakeHandler",
        input_readers.__name__ + ".GoogleCloudStorageRecordInputReader",
        {
            "input_reader": {
                "bucket_name": bucket_name,
                "objects": [test_filename]
            }
        },
        shard_count=4,
      )

    test_support.execute_until_empty(self.taskqueue)
    self.assertEqual(100, len(FakeHandler.processed_entites))

  def testHugeTaskPayloadTest(self):
    """Test map job with huge parameter values."""
    input_data = [str(i).encode() for i in range(100)]

    bucket_name = "byates"
    test_filename = "testfile"

    bucket = _storage_client.get_bucket(bucket_name)
    blob = bucket.blob(test_filename)
    
    with blob.open("wb") as f:
      with records.RecordsWriter(f) as w:
        for record in input_data:
          w.write(record)

    control.start_map(
        "fake_map",
        __name__ + ".FakeHandler",
        input_readers.__name__ + ".GoogleCloudStorageRecordInputReader",
        {
            "input_reader": {
                "bucket_name": bucket_name,
                "objects": [test_filename],
                # the parameter will be compressed and should fit into
                # taskqueue payload
                "huge_parameter": "0" * 200000,  # 200K
            }
        },
        shard_count=4,
      )

    test_support.execute_until_empty(self.taskqueue)
    self.assertEqual(100, len(FakeHandler.processed_entites))
    self.assertEqual([], model._HugeTaskPayload.all().fetch(100))

  def testHugeTaskUseDatastore(self):
    """Test map job with huge parameter values."""
    input_data = [str(i).encode() for i in range(100)]

    bucket_name = "byates"
    test_filename = "testfile"

    bucket = _storage_client.get_bucket(bucket_name)
    blob = bucket.blob(test_filename)

    with blob.open("wb") as f:
      with records.RecordsWriter(f) as w:
        for record in input_data:
          w.write(record)

    control.start_map(
        "fake_map",
        __name__ + ".FakeHandler",
        input_readers.__name__ + ".GoogleCloudStorageRecordInputReader",
        {
            "input_reader": {
                "bucket_name": bucket_name,
                "objects": [test_filename],
                # the parameter can't be compressed and wouldn't fit into
                # taskqueue payload
                "huge_parameter": random_string(900000)
            }
        },
        shard_count=4,
      )

    test_support.execute_until_empty(self.taskqueue)
    self.assertEqual(100, len(FakeHandler.processed_entites))
    self.assertEqual([], model._HugeTaskPayload.all().fetch(100))


class GCSOutputWriterTestBase(testutil.CloudStorageTestBase):
  """Base class for all GCS output writer tests."""

  def setUp(self):
    super().setUp()
    self.original_slice_duration = parameters.config._SLICE_DURATION_SEC
    # Use this to adjust what is printed for debugging purpose.
    logging.getLogger().setLevel(logging.CRITICAL)
    self.writer_cls = output_writers._GoogleCloudStorageOutputWriter

    # Populate datastore with inputs.
    entity_count = 30
    for i in range(entity_count):
      FakeEntity(int_property=i).put()

    # Make slice short.
    parameters.config._SLICE_DURATION_SEC = 1
    # 5 items per second. This effectively terminates a slice after
    # processing 5 items.
    self.processing_rate = 5

  def tearDown(self):
    parameters.config._SLICE_DURATION_SEC = self.original_slice_duration
    super().tearDown()


class GCSOutputWriterNoDupModeTest(GCSOutputWriterTestBase):
  """Test GCS output writer slice recovery."""

  def testSliceRecoveryWithForcedFlushing(self):
    mr_id = control.start_map(
        "test_map",
        __name__ + ".FaultyHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "input_reader": {
                "entity_kind": __name__ + "." + FakeEntity.__name__,
            },
            "output_writer": {
                "bucket_name": self.TEST_BUCKET,
                "no_duplicate": True,
            },
            "processing_rate": self.processing_rate,
        },
        output_writer_spec=(output_writers.__name__ +
                            "._GoogleCloudStorageOutputWriter"),
        shard_count=1)

    test_support.execute_until_empty(self.taskqueue)
    mr_state = model.MapreduceState.get_by_job_id(mr_id)
    # Verify MR is successful.
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     mr_state.result_status)

    # Read output info from shard states.
    shard_state = next(model.ShardState.find_all_by_mapreduce_state(mr_state))
    writer_state = shard_state.writer_state
    last_seg_index = writer_state[self.writer_cls._LAST_SEG_INDEX]
    seg_prefix = writer_state[self.writer_cls._SEG_PREFIX]

    # Verify we have 5 segs.
    self.assertEqual(4, last_seg_index)

    self._assertOutputEqual(seg_prefix, last_seg_index)

    # Check there are indeed duplicated data.
    bucket = _storage_client.get_bucket(self.TEST_BUCKET)
    f1 = {line for line in bucket.blob(seg_prefix + "0").open("rb")}
    f2 = {line for line in bucket.blob(seg_prefix + "1").open("rb")}
    common = f1.intersection(f2)
    self.assertEqual({"10\n", "11\n"}, common)

  def testSliceRecoveryWithFrequentFlushing(self):
    mr_id = control.start_map(
        "test_map",
        __name__ + ".FaultyHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "input_reader": {
                "entity_kind": __name__ + "." + FakeEntity.__name__,
            },
            "output_writer": {
                "bucket_name": "bucket",
                "no_duplicate": True,
            },
            "processing_rate": self.processing_rate,
        },
        output_writer_spec=(output_writers.__name__ +
                            "._GoogleCloudStorageOutputWriter"),
        shard_count=1)

    test_support.execute_until_empty(self.taskqueue)
    mr_state = model.MapreduceState.get_by_job_id(mr_id)
    # Verify MR is successful.
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     mr_state.result_status)

    # Read output info from shard states.
    shard_state = next(model.ShardState.find_all_by_mapreduce_state(mr_state))
    writer_state = shard_state.writer_state
    last_seg_index = writer_state[self.writer_cls._LAST_SEG_INDEX]
    seg_prefix = writer_state[self.writer_cls._SEG_PREFIX]

    # Verify we have 5 segs.
    self.assertEqual(4, last_seg_index)

    self._assertOutputEqual(seg_prefix, last_seg_index)

  def testSliceRecoveryWithNoFlushing(self):
    # Flushing is done every 256K, which means never until slice recovery.
    mr_id = control.start_map(
        "test_map",
        __name__ + ".FaultyHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "input_reader": {
                "entity_kind": __name__ + "." + FakeEntity.__name__,
            },
            "output_writer": {
                "bucket_name": "bucket",
                "no_duplicate": True,
            },
            "processing_rate": self.processing_rate,
        },
        output_writer_spec=(output_writers.__name__ +
                            "._GoogleCloudStorageOutputWriter"),
        shard_count=1)

    test_support.execute_until_empty(self.taskqueue)
    mr_state = model.MapreduceState.get_by_job_id(mr_id)
    # Verify MR is successful.
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     mr_state.result_status)

    # Read output info from shard states.
    shard_state = next(model.ShardState.find_all_by_mapreduce_state(mr_state))
    writer_state = shard_state.writer_state
    last_seg_index = writer_state[self.writer_cls._LAST_SEG_INDEX]
    seg_prefix = writer_state[self.writer_cls._SEG_PREFIX]

    # Verify we have 5 segs.
    self.assertEqual(4, last_seg_index)

    self._assertOutputEqual(seg_prefix, last_seg_index)

  def _assertOutputEqual(self, seg_prefix, last_seg_index):
    # Read back outputs.
    reader = gcs_file_seg_reader._GCSFileSegReader(seg_prefix, last_seg_index)
    result = ""
    while True:
      tmp = reader.read(n=100)
      if not tmp:
        break
      result += tmp

    # Verify output has no duplicates.
    expected = ""
    for i in range(30):
      expected += "%s\n" % i
    self.assertEqual(expected, result)


class FaultyHandler:

  def __init__(self):
    self.slice_count = 0

  # pylint: disable=unused-argument
  def __setstate__(self, state):
    # Reset at beginning of each slice.
    self.slice_count = 0

  def __call__(self, entity):
    self.slice_count += 1
    yield "%s\n" % entity.int_property
    slice_id = context.get()._shard_state.slice_id
    # Raise exception when processing the 2 item in a slice every 3 slices.
    if (self.slice_count == 2 and
        (slice_id + 1) % 3 == 0):
      raise Exception("Intentionally raise an exception")


