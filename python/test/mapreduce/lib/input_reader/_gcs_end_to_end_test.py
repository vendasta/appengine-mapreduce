#!/usr/bin/env python
"""End-to-end tests for _gcs.py."""

# Using opensource naming conventions, pylint: disable=g-bad-name

from mapreduce import model
from mapreduce import parameters
from mapreduce import test_support
from testlib import testutil
from mapreduce.api import map_job
from mapreduce.lib import input_reader

# Global for collecting data across all map shards
_memory_mapper_data = []
_processed_count = 0


class _InputReaderMemoryMapper(map_job.Mapper):

  def __call__(self, ctx, val):
    _memory_mapper_data.append(val.read())


class _MyPathFilter(input_reader.PathFilter):

  def accept(self, slice_ctx, path):
    global _processed_count
    _processed_count += 1
    if _processed_count == 1:
      return False
    return True


class GCSInputReaderEndToEndTest(testutil.CloudStorageTestBase, testutil.HandlerTestBase):
  """End-to-end tests for GoogleCloudStorageInputReader."""

  def setUp(self):
    super().setUp()
    # clear global list of mapped data
    global _memory_mapper_data
    _memory_mapper_data = []
    global _processed_count
    _processed_count = 0
    self.original_slice_duration_sec = parameters.config._SLICE_DURATION_SEC

  def tearDown(self):
    parameters.config._SLICE_DURATION_SEC = self.original_slice_duration_sec

  def create_test_content(self, object_prefix, num_files):
    """Create a file in Google Cloud Storage with a small amount of content.

    Args:
      object_prefix: a string prefix for each object/file that will be created.
        A suffix with a file number will automatically be appended.
      num_files: the number of files to create.

    Returns:
      A list with each element containing the data in one of the created files.
    """
    created_content = []
    created_content = []
    for file_num in range(num_files):
        content = f"Dummy Content {file_num}"
        created_content.append(content)
        blob = self.bucket.blob(f"{object_prefix}{file_num:03d}")
        blob.upload_from_string(content)
    return created_content

  def _run_test(self, num_shards, num_files, multi_slices=False):
    object_prefix = f"{self.gcsPrefix}/file-"
    job_name = self.gcsPrefix
    expected_content = self.create_test_content(object_prefix,
                                                num_files)
    job = map_job.Job.submit(map_job.JobConfig(
        job_name=job_name,
        mapper=_InputReaderMemoryMapper,
        input_reader_cls=input_reader.GCSInputReader,
        input_reader_params={"bucket_name": self.TEST_BUCKET,
                             "objects": [object_prefix + "*"],
                             "path_filter": _MyPathFilter()},
        shard_count=num_shards))

    test_support.execute_until_empty(self.taskqueue)
    self.assertEqual(expected_content.sort(), _memory_mapper_data.sort())
    self.assertEqual(job.SUCCESS, job.get_status())
    self.assertEqual(
        num_files - 1,
        job.get_counter(input_reader.GCSInputReader.COUNTER_FILE_READ))
    if multi_slices:
      ss = model.ShardState.find_all_by_mapreduce_state(job._state)
      for s in ss:
        self.assertTrue(s.slice_id > 0)

  def testSingleShard(self):
    self._run_test(num_shards=1, num_files=10)

  def testSingleShardMultiSlices(self):
    # Force a new slice on every item processed.
    parameters.config._SLICE_DURATION_SEC = -1
    self._run_test(num_shards=1, num_files=10, multi_slices=True)

  def testMultipleShards(self):
    self._run_test(num_shards=4, num_files=10)

  def testMultipleShardsMultiSlices(self):
    # Force a new slice on every item processed.
    parameters.config._SLICE_DURATION_SEC = -1
    self._run_test(num_shards=4, num_files=10, multi_slices=True)

