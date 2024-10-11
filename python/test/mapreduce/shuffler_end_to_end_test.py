#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.

import pipeline

from mapreduce import base_handler
from mapreduce import kv_pb
from mapreduce import mapreduce_pipeline
from mapreduce import output_writers
from mapreduce import records
from mapreduce import shuffler
from mapreduce import test_support
from testlib import testutil


class HashEndToEndTest(testutil.CloudStorageTestBase, testutil.HandlerTestBase):
  """End-to-end test for _HashPipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  # pylint: disable=invalid-name
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testHashingMultipleFiles(self):
    """Test hashing files."""
    input_data = [(str(i), str(i)) for i in range(100)]
    input_data.sort()

    test_filename = f"{self.gcsPrefix}/testfile"
    full_filename = f"/{self.TEST_BUCKET}/{test_filename}"

    blob = self.bucket.blob(test_filename)
    
    with blob.open("wb") as f:
      with records.RecordsWriter(f) as w:
        for (k, v) in input_data:
          proto = kv_pb.KeyValue()
          proto.key = k
          proto.value = v
          w.write(proto.SerializeToString())

    p = shuffler._HashPipeline(self.gcsPrefix, self.TEST_BUCKET,
                               [full_filename, full_filename, full_filename])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    p = shuffler._HashPipeline.from_id(p.pipeline_id)

    list_of_output_files = p.outputs.default.value
    output_data = []
    for output_files in list_of_output_files:
      for output_file in output_files:
        blob = self.bucket.blob(output_file)
        with blob.open("rb") as f:
          for binary_record in records.RecordsReader(f):
            proto = kv_pb.KeyValue()
            proto.ParseFromString(binary_record)
            output_data.append((proto.key, proto.value))

    output_data.sort()
    self.assertEqual(300, len(output_data))
    for i in range(len(input_data)):
      self.assertEqual(input_data[i], output_data[(3 * i)])
      self.assertEqual(input_data[i], output_data[(3 * i) + 1])
      self.assertEqual(input_data[i], output_data[(3 * i) + 2])
    self.assertEqual(1, len(self.emails))


class SortFileEndToEndTest(testutil.CloudStorageTestBase, testutil.HandlerTestBase):
  """End-to-end test for _SortFilePipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  # pylint: disable=invalid-name
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testSortFile(self):
    """Test sorting a file."""
    test_filename = f"{self.gcsPrefix}/testfile"
    full_filename = f"/{self.TEST_BUCKET}/{test_filename}"

    input_data = [
        (str(i), "_" + str(i)) for i in range(100)]

    with self.bucket.blob(test_filename).open("wb") as f:
      with records.RecordsWriter(f) as w:
        for (k, v) in input_data:
          proto = kv_pb.KeyValue()
          proto.key = k
          proto.value = v
          w.write(proto.SerializeToString())

    p = shuffler._SortChunksPipeline(self.gcsPrefix, self.TEST_BUCKET, [[full_filename]])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    p = shuffler._SortChunksPipeline.from_id(p.pipeline_id)

    input_data.sort()
    output_files = p.outputs.default.value[0]
    output_data = []
    for output_file in output_files:
      output_file = output_file[len(f"/{self.TEST_BUCKET}/"):]
      with self.bucket.blob(output_file).open("rb") as f:
        for binary_record in records.RecordsReader(f):
          proto = kv_pb.KeyValue()
          proto.ParseFromString(binary_record)
          output_data.append((proto.key, proto.value))

    self.assertEqual(input_data, output_data)
    self.assertEqual(1, len(self.emails))


# pylint: disable=invalid-name
def fake_handler_yield_str(key, value, partial):
  """Test handler that yields parameters converted to string."""
  yield str((key, value, partial))


class FakeMergePipeline(base_handler.PipelineBase):
  """A pipeline to merge-sort multiple sorted files.

  Args:
    bucket_name: The name of the Google Cloud Storage bucket.
    filenames: list of input file names as string. Each file is of records
    format with kv_pb.KeyValue protocol messages. All files should
    be sorted by key value.

  Returns:
    The list of filenames as string. Resulting files contain records with
    str((key, values)) obtained from MergingReader.
  """

  def run(self, name, bucket_name, filenames):
    yield mapreduce_pipeline.MapperPipeline(
        name,
        __name__ + ".fake_handler_yield_str",
        shuffler.__name__ + "._MergingReader",
        output_writers.__name__ + "._GoogleCloudStorageRecordOutputWriter",
        params={
            shuffler._MergingReader.FILES_PARAM: [filenames],
            shuffler._MergingReader.MAX_VALUES_COUNT_PARAM:
                shuffler._MergePipeline._MAX_VALUES_COUNT,
            shuffler._MergingReader.MAX_VALUES_SIZE_PARAM:
                shuffler._MergePipeline._MAX_VALUES_SIZE,
            "output_writer": {
                "bucket_name": bucket_name,
            },
        },
        )


class MergingReaderEndToEndTest(testutil.CloudStorageTestBase, testutil.HandlerTestBase):
  """End-to-end test for MergingReader."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  # pylint: disable=invalid-name
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testMergeFiles(self):
    """Test merging multiple files."""
    input_data = [(str(i), "_" + str(i)) for i in range(100)]
    input_data.sort()

    test_filename = f"{self.gcsPrefix}/testfile"
    full_filename = f"/{self.TEST_BUCKET}/{test_filename}"

    with self.bucket.blob(test_filename).open("wb") as f:
      with records.RecordsWriter(f) as w:
        for (k, v) in input_data:
          proto = kv_pb.KeyValue()
          proto.key = k
          proto.value = v
          w.write(proto.SerializeToString())

    p = FakeMergePipeline(self.gcsPrefix, self.TEST_BUCKET,
                          [full_filename, full_filename, full_filename])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    p = FakeMergePipeline.from_id(p.pipeline_id)

    output_file = p.outputs.default.value[0]
    output_data = []
    with self.bucket.blob(output_file).open("rb") as f:
      for record in records.RecordsReader(f):
        output_data.append(record)

    expected_data = [
        str((k, [v, v, v], False)) for (k, v) in input_data]
    self.assertEqual(expected_data, output_data)
    self.assertEqual(1, len(self.emails))

  def testPartialRecords(self):
    """Test merging into partial key values."""
    try:
      self._prev_max_values_count = shuffler._MergePipeline._MAX_VALUES_COUNT
      # force max values count to extremely low value.
      shuffler._MergePipeline._MAX_VALUES_COUNT = 1

      input_data = [("1", "a"), ("2", "b"), ("3", "c")]
      input_data.sort()

      test_filename = f"{self.gcsPrefix}/testfile"
      full_filename = f"/{self.TEST_BUCKET}/{test_filename}"

      blob = self.bucket.blob(test_filename)
      with blob.open("wb") as f:
        with records.RecordsWriter(f) as w:
          for (k, v) in input_data:
            proto = kv_pb.KeyValue()
          proto.key = k
          proto.value = v
          w.write(proto.SerializeToString())

      p = FakeMergePipeline(self.gcsPrefix, self.TEST_BUCKET,
                            [full_filename, full_filename, full_filename])
      p.start()
      test_support.execute_until_empty(self.taskqueue)
      p = FakeMergePipeline.from_id(p.pipeline_id)

      output_file = p.outputs.default.value[0]
      output_data = []
      blob = self.bucket.blob(output_file)
      with blob.open("rb") as f:
        for record in records.RecordsReader(f):
          output_data.append(record)

      expected_data = [
          ("1", ["a"], True),
          ("1", ["a"], True),
          ("1", ["a"], False),
          ("2", ["b"], True),
          ("2", ["b"], True),
          ("2", ["b"], False),
          ("3", ["c"], True),
          ("3", ["c"], True),
          ("3", ["c"], False),
          ]
      self.assertEqual([str(e) for e in expected_data], output_data)
    finally:
      shuffler._MergePipeline._MAX_VALUES_COUNT = self._prev_max_values_count
    self.assertEqual(1, len(self.emails))


class ShuffleEndToEndTest(testutil.CloudStorageTestBase, testutil.HandlerTestBase):
  """End-to-end test for ShufflePipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  # pylint: disable=invalid-name
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testShuffleNoData(self):
    test_filename = f"{self.gcsPrefix}/testfile"
    full_filename = f"/{self.TEST_BUCKET}/{test_filename}"

    blob = self.bucket.blob(test_filename)
    gcs_file = blob.open("wb")
    gcs_file.write(b"")
    gcs_file.close()

    p = shuffler.ShufflePipeline(self.gcsPrefix, {"bucket_name": self.TEST_BUCKET},
                                 [full_filename, full_filename, full_filename])
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = shuffler.ShufflePipeline.from_id(p.pipeline_id)
    for filename in p.outputs.default.value:
      blob = self.bucket.blob(filename)
      blob.reload()
      self.assertEqual(0, blob.size)
    self.assertEqual(1, len(self.emails))

  def testShuffleNoFile(self):
    p = shuffler.ShufflePipeline(self.gcsPrefix, {"bucket_name": self.TEST_BUCKET}, [])
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = shuffler.ShufflePipeline.from_id(p.pipeline_id)
    for filename in p.outputs.default.value:
      blob = self.bucket.blob(filename)
      blob.reload()
      self.assertEqual(0, blob.size)
    self.assertEqual(1, len(self.emails))

  def testShuffleFiles(self):
    """Test shuffling multiple files."""
    input_data = [(str(i), str(i)) for i in range(100)]
    input_data.sort()

    test_filename = f"{self.gcsPrefix}/testfile"
    full_filename = f"/{self.TEST_BUCKET}/{test_filename}"

    blob = self.bucket.blob(test_filename)
    with blob.open("wb") as f:
      with records.RecordsWriter(f) as w:
        for (k, v) in input_data:
          proto = kv_pb.KeyValue()
          proto.key = k
          proto.value = v
          w.write(proto.SerializeToString())

    p = shuffler.ShufflePipeline(self.gcsPrefix, {"bucket_name": self.TEST_BUCKET},
                                 [full_filename, full_filename, full_filename])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    p = shuffler.ShufflePipeline.from_id(p.pipeline_id)

    output_files = p.outputs.default.value
    output_data = []
    for output_file in output_files:
      blob = self.bucket.blob(output_file)
      with blob.open("rb") as f:
        for record in records.RecordsReader(f):
          proto = kv_pb.KeyValues()
          proto.ParseFromString(record)
          output_data.append((proto.key, proto.value))
    output_data.sort()

    expected_data = sorted([
        (str(k), [str(v), str(v), str(v)]) for (k, v) in input_data])
    self.assertEqual(expected_data, output_data)
    self.assertEqual(1, len(self.emails))

