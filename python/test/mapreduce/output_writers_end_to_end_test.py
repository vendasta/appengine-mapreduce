#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.




# Using opensource naming conventions, pylint: disable=g-bad-name

import unittest

from google.appengine.ext import db

from mapreduce import control
from mapreduce import input_readers
from mapreduce import model
from mapreduce import output_writers
from mapreduce import records
from mapreduce import test_support
from testlib import testutil

from google.cloud import storage

storage_client = storage.Client()

DATASTORE_READER_NAME = (input_readers.__name__ + "." +
                         input_readers.DatastoreInputReader.__name__)


class FakeEntity(db.Model):
  """Test entity class."""


def fake_handler_yield_key_str(entity):
  """Test handler which yields entity key."""
  yield f"{entity.key}\n".encode()


class GoogleCloudStorageOutputWriterEndToEndTest(testutil.CloudStorageTestBase):
  """End-to-end tests for CloudStorageOutputWriter."""

  WRITER_CLS = output_writers._GoogleCloudStorageOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__

  def _runTest(self, num_shards):
    entity_count = 1000
    job_name = "test_map"

    for _ in range(entity_count):
      FakeEntity().put()

    mapreduce_id = control.start_map(
        job_name,
        __name__ + ".fake_handler_yield_key_str",
        DATASTORE_READER_NAME,
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
            "output_writer": {
                "bucket_name": self.TEST_BUCKET,
            },
        },
        shard_count=num_shards,
        output_writer_spec=self.WRITER_NAME)

    test_support.execute_until_empty(self.taskqueue)
    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = self.WRITER_CLS.get_filenames(mapreduce_state)

    self.assertEqual(num_shards, len(set(filenames)))
    total_entries = 0
    for shard in range(num_shards):
      self.assertTrue(filenames[shard].startswith(job_name))
      bucket = storage_client.get_bucket(self.TEST_BUCKET)
      blob = bucket.blob(filenames[shard])
      data = blob.download_as_string()
      # strip() is used to remove the last newline of each file so that split()
      # does not retrun extraneous empty entries.
      total_entries += len(data.strip().split(b"\n"))
    self.assertEqual(entity_count, total_entries)

  def testSingleShard(self):
    self._runTest(num_shards=1)

  def testMultipleShards(self):
    self._runTest(num_shards=4)


class GCSRecordOutputWriterEndToEndTestBase(testutil.CloudStorageTestBase):

  WRITER_CLS = output_writers._GoogleCloudStorageRecordOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__

  def _runTest(self, num_shards):
    entity_count = 1000
    job_name = "test_map"

    for _ in range(entity_count):
      FakeEntity().put()

    mapreduce_id = control.start_map(
        job_name,
        __name__ + ".fake_handler_yield_key_str",
        DATASTORE_READER_NAME,
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
            "output_writer": {
                "bucket_name": self.TEST_BUCKET,
            },
        },
        shard_count=num_shards,
        output_writer_spec=self.WRITER_NAME)

    test_support.execute_until_empty(self.taskqueue)
    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = self.WRITER_CLS.get_filenames(mapreduce_state)

    self.assertEqual(num_shards, len(set(filenames)))
    total_entries = 0
    for shard in range(num_shards):
      self.assertTrue(filenames[shard].startswith(job_name))
      bucket = storage_client.get_bucket(self.TEST_BUCKET)
      data = b"".join([_ for _ in records.RecordsReader(
          bucket.blob(filenames[shard]).open("rb"))])
      # strip() is used to remove the last newline of each file so that split()
      # does not return extraneous empty entries.
      total_entries += len(data.strip().split(b"\n"))
    self.assertEqual(entity_count, total_entries)

  def testSingleShard(self):
    self._runTest(num_shards=1)

  def testMultipleShards(self):
    self._runTest(num_shards=4)


class GoogleCloudStorageRecordOutputWriterEndToEndTest(
    GCSRecordOutputWriterEndToEndTestBase,
    testutil.CloudStorageTestBase):
  """End-to-end tests for CloudStorageRecordOutputWriter."""

  WRITER_CLS = output_writers._GoogleCloudStorageRecordOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__


class GoogleCloudStorageConsistentRecordOutputWriterEndToEndTest(
    GCSRecordOutputWriterEndToEndTestBase,
    testutil.CloudStorageTestBase):
  """End-to-end tests for CloudStorageConsistentRecordOutputWriter."""

  WRITER_CLS = output_writers.GoogleCloudStorageConsistentRecordOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__


class GoogleCloudStorageConsistentOutputWriterEndToEndTest(
    testutil.CloudStorageTestBase):
  """End-to-end tests for CloudStorageOutputWriter."""

  WRITER_CLS = output_writers.GoogleCloudStorageConsistentOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__

  def _runTest(self, num_shards):
    entity_count = 1000
    job_name = "test_map"

    for _ in range(entity_count):
      FakeEntity().put()

    mapreduce_id = control.start_map(
        job_name,
        __name__ + ".fake_handler_yield_key_str",
        DATASTORE_READER_NAME,
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
            "output_writer": {
                "bucket_name": self.TEST_BUCKET,
                "tmp_bucket_name": self.TEST_TMP_BUCKET,
            },
        },
        shard_count=num_shards,
        output_writer_spec=self.WRITER_NAME)

    test_support.execute_until_empty(self.taskqueue)
    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = self.WRITER_CLS.get_filenames(mapreduce_state)

    self.assertEqual(num_shards, len(set(filenames)))
    total_entries = 0
    for shard in range(num_shards):
      self.assertTrue(filenames[shard].startswith("/{}/{}".format(self.TEST_BUCKET,
                                                              job_name)))
      data = cloudstorage.open(filenames[shard]).read()
      # strip() is used to remove the last newline of each file so that split()
      # does not retrun extraneous empty entries.
      total_entries += len(data.strip().split("\n"))
    self.assertEqual(entity_count, total_entries)

    # no files left in tmpbucket
    self.assertFalse(list(cloudstorage.listbucket("/%s" % self.TEST_BUCKET)))
    # and only expected files in regular bucket
    files_in_bucket = [
        f.filename for f in cloudstorage.listbucket("/%s" % bucket_name)]
    self.assertEqual(filenames, files_in_bucket)

  def testSingleShard(self):
    self._runTest(num_shards=1)

  def testMultipleShards(self):
    self._runTest(num_shards=4)


