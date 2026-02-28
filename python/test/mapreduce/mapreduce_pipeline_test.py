#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.

import pipeline
from google.cloud import ndb
from google.cloud import storage

from mapreduce import errors
from mapreduce import input_readers
from mapreduce import mapreduce_pipeline
from mapreduce import model
from mapreduce import output_writers
from mapreduce import records
from mapreduce import test_support
from testlib import testutil


class FakeEntity(ndb.Model):
  data = ndb.StringProperty()


class FakeOutputEntity(ndb.Model):
  data = ndb.StringProperty()


class RetryCount(ndb.Model):
  retries = ndb.IntegerProperty()


def fake_mapreduce_map(entity):
  yield (entity.data, "")


def fake_mapreduce_reduce(key, values):
  yield str((key, values))


def fake_failed_map(_):
  raise errors.FailJobError()


class FakeFileRecordsOutputWriter(
    output_writers._GoogleCloudStorageRecordOutputWriter):

  RETRIES = 11

  def finalize(self, ctx, shard_state):
    retry_count = RetryCount.get_by_id(__name__)
    if not retry_count:
      retry_count = RetryCount(id=__name__, retries=0)
    if retry_count.retries < self.RETRIES:
      retry_count.retries += 1
      retry_count.put()
      raise Exception("output writer finalize failed.")
    super().finalize(ctx, shard_state)


class MapreducePipelineTest(testutil.CloudStorageTestBase, testutil.HandlerTestBase):

  def testFailedMapReduce(self):
    max_attempts_before = pipeline.pipeline._DEFAULT_MAX_ATTEMPTS
    try:
      pipeline.pipeline._DEFAULT_MAX_ATTEMPTS = 1

      entity_count = 200

      for i in range(entity_count):
        FakeEntity(data=str(i)).put()
        FakeEntity(data=str(i)).put()

      p = mapreduce_pipeline.MapreducePipeline(
          self.gcsPrefix,
          __name__ + ".test_failed_map",
          __name__ + ".test_mapreduce_reduce",
          input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
          output_writer_spec=(output_writers.__name__ +
                              "._GoogleCloudStorageRecordOutputWriter"),
          mapper_params={
              "entity_kind": __name__ + "." + FakeEntity.__name__,
          },
          reducer_params={
              "output_writer": {
                  "bucket_name": self.TEST_BUCKET
              },
          },
          shards=3)
      p.max_attempts = 1
      p.start()
      test_support.execute_until_empty(self.taskqueue)

      p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
      self.assertTrue(p.was_aborted)
    finally:
      pipeline.pipeline._DEFAULT_MAX_ATTEMPTS = max_attempts_before

  def testMapReduce(self):
    self.skipTest(
        "MapReduce shuffle uses consistent writer which is not supported with cloud storage SDK")
    job_name = self.gcsPrefix
    entity_count = 200

    for i in range(entity_count):
      FakeEntity(data=str(i)).put()
      FakeEntity(data=str(i)).put()

    p = mapreduce_pipeline.MapreducePipeline(
        job_name,
        __name__ + ".fake_mapreduce_map",
        __name__ + ".fake_mapreduce_reduce",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=(
            output_writers.__name__ + "._GoogleCloudStorageRecordOutputWriter"),
        mapper_params={
            "entity_kind": __name__ + "." + FakeEntity.__name__,
            "bucket_name": self.TEST_BUCKET
        },
        reducer_params={
            "output_writer": {
                "bucket_name": self.TEST_BUCKET
            },
        },
        shards=16)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertFalse(p.was_aborted)

    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     p.outputs.result_status.value)
    output_data = []
    for output_file in p.outputs.default.value:
      with self.bucket.blob(output_file).open("rb") as f:
        for record in records.RecordsReader(f):
          output_data.append(record)

    expected_data = [
        str((str(d), ["", ""])) for d in range(entity_count)]
    expected_data.sort()
    output_data.sort()
    self.assertEqual(expected_data, output_data)

    _storage_client = storage.Client()
    blobs = list(_storage_client.list_blobs(self.TEST_BUCKET, prefix=job_name))
    for blob in blobs:
      if blob.name:
        self.assertFalse(
            blob.name.startswith("%s-shuffle-" % job_name))

  def testMapReduceWithShardRetry(self):
    self.skipTest(
        "MapReduce shuffle uses consistent writer which is not supported with cloud storage SDK")
    entity_count = 200
    ndb.delete_multi(RetryCount.query().fetch(keys_only=True))

    for i in range(entity_count):
      FakeEntity(data=str(i)).put()
      FakeEntity(data=str(i)).put()

    p = mapreduce_pipeline.MapreducePipeline(
        "test",
        __name__ + ".fake_mapreduce_map",
        __name__ + ".fake_mapreduce_reduce",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=(
            __name__ + ".FakeFileRecordsOutputWriter"),
        mapper_params={
            "input_reader": {
                "entity_kind": __name__ + "." + FakeEntity.__name__,
            },
        },
        reducer_params={
            "output_writer": {
                "bucket_name": self.TEST_BUCKET
            },
        },
        shards=16)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertFalse(p.was_aborted)

    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     p.outputs.result_status.value)
    output_data = []
    retries = 0
    for output_file in p.outputs.default.value:
      retries += (int(output_file[-1]) - 1)
      with self.bucket.blob(output_file).open("rb") as f:
        for record in records.RecordsReader(f):
          output_data.append(record)

    self.assertEqual(44, retries)
    expected_data = [
        str((str(d), ["", ""])) for d in range(entity_count)]
    expected_data.sort()
    output_data.sort()
    self.assertEqual(expected_data, output_data)
