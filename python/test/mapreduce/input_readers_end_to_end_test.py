#!/usr/bin/env python




# Using opensource naming conventions, pylint: disable=g-bad-name

import unittest

from mapreduce import control
from mapreduce import input_readers
from mapreduce import model
from mapreduce import test_support
from testlib import testutil

from google.cloud import storage

storage_client = storage.Client()


# Global for collecting data across all map shards
_memory_mapper_data = []


def _input_reader_memory_mapper(data):
  _memory_mapper_data.append(data.read())


class GoogleCloudStorageInputReaderEndToEndTest(testutil.CloudStorageTestBase):
  """End-to-end tests for GoogleCloudStorageInputReader."""

  def _ClearMapperData(self):
    """Clear global list of mapped data."""
    global _memory_mapper_data
    _memory_mapper_data = []

  def setUp(self):
    super().setUp()
    self._ClearMapperData()

  def create_test_content(self, bucket_name, object_prefix, num_files):
    """Create a file in Google Cloud Storage with a small amount of content.

    Args:
      bucket_name: the name of the bucket, with no delimiters.
      object_prefix: a string prefix for each object/file that will be created.
        A suffix with a file number will automatically be appended.
      num_files: the number of files to create.

    Returns:
      A list with each element containing the data in one of the created files.
    """
    bucket = storage_client.get_bucket(bucket_name)
    created_content = []
    for file_num in range(num_files):
        content = "Dummy Content %d" % file_num
        created_content.append(content)
        blob = bucket.blob(f"{object_prefix}{file_num:03d}")
        blob.upload_from_string(content)
    return created_content

  def _run_test(self, num_shards, num_files):
    # bucket_name = "testing"
    bucket_name = "byates"
    object_prefix = "file-"
    job_name = "test_map"
    input_class = (input_readers.__name__ + "." +
                   input_readers._GoogleCloudStorageInputReader.__name__)

    expected_content = self.create_test_content(bucket_name,
                                                object_prefix,
                                                num_files)

    control.start_map(
        job_name,
        __name__ + "." + "_input_reader_memory_mapper",
        input_class,
        {
            "input_reader": {
                "bucket_name": bucket_name,
                "objects": [object_prefix + "*"]
            },
        },
        shard_count=num_shards)

    test_support.execute_until_empty(self.taskqueue)
    self.assertEqual(expected_content.sort(), _memory_mapper_data.sort())

  def testSingleShard(self):
    self._run_test(num_shards=1, num_files=10)

  def testMultipleShards(self):
    self._run_test(num_shards=4, num_files=10)

  def testStrict(self):
    """Tests that fail_on_missing_input works properly."""
    gcs_files = []
    bucket = storage_client.get_bucket("byates")
    for num in range(10):
        gcs_file = f"file{num}"
        blob = bucket.blob(gcs_file)
        blob.upload_from_string(str(num + 100))
        gcs_files.append(gcs_file)

    input_class = (input_readers.__name__ + "." +
                   input_readers._GoogleCloudStorageInputReader.__name__)

    def _RunMR(fail_on_missing_input=None):
      """Clears the state and runs a single (strict or not) MR."""
      self._ClearMapperData()

      input_reader_dict = {
          "bucket_name": bucket.name,
          "objects": gcs_files,
      }
      if fail_on_missing_input is not None:
        input_reader_dict["fail_on_missing_input"] = fail_on_missing_input
      mr_id = control.start_map(
          "job1",
          __name__ + "." + "_input_reader_memory_mapper",
          input_class,
          {
              "input_reader": input_reader_dict,
          },
          shard_count=10)
      test_support.execute_until_empty(self.taskqueue)
      return mr_id

    # All files are there. Default, strict and non-strict MRs should work.
    _RunMR(None)
    self.assertEqual([str(num + 100).encode() for num in range(10)],
                     sorted(_memory_mapper_data))
    _RunMR(False)
    self.assertEqual([str(num + 100).encode() for num in range(10)],
                     sorted(_memory_mapper_data))
    _RunMR(True)
    self.assertEqual([str(num + 100).encode() for num in range(10)],
                     sorted(_memory_mapper_data))

    # Now remove a file.
    bucket.delete_blob("file5")

    # Non-strict MR still works but some output is not there.
    mr_id = _RunMR(False)
    self.assertEqual([str(num + 100).encode() for num in [0, 1, 2, 3, 4, 6, 7, 8, 9]],
                     sorted(_memory_mapper_data))
    self.assertEqual(model.MapreduceState.get_by_job_id(mr_id).result_status,
                      model.MapreduceState.RESULT_SUCCESS)

    # Strict MR fails.
    mr_id = _RunMR(True)
    self.assertEqual(model.MapreduceState.get_by_job_id(mr_id).result_status,
                      model.MapreduceState.RESULT_FAILED)


