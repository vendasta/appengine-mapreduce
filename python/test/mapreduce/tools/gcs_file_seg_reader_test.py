#!/usr/bin/env python
"""Tests for gcs_file_seg_reader."""

# pylint: disable=g-bad-name

import unittest

from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import testbed

from mapreduce import output_writers
from mapreduce.tools import gcs_file_seg_reader

from google.cloud import storage

_storage_client = storage.Client()


class GCSFileSegReaderTest(unittest.TestCase):
  """Test GCSFileSegReader."""

  def setUp(self):
    super().setUp()

    self.testbed = testbed.Testbed()
    self.testbed.activate()

    self.testbed.init_app_identity_stub()
    self.testbed.init_blobstore_stub()
    # HRD with no eventual consistency.
    policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
    self.testbed.init_datastore_v3_stub(consistency_policy=policy)
    self.testbed.init_memcache_stub()
    self.testbed.init_urlfetch_stub()

    self.writer_cls = output_writers._GoogleCloudStorageOutputWriter

    self.bucket_name = "byates"
    self.bucket = _storage_client.get_bucket(self.bucket_name)
    self.seg_prefix = f"{self._testMethodName}/prefix-"

  def tearDown(self):
    self.testbed.deactivate()
    super().tearDown()

  def testMissingMetadata(self):
    f = self.bucket.blob(self.seg_prefix + "0").open("w")
    f.write("abc")
    f.close()

    with self.assertRaises(ValueError):
        gcs_file_seg_reader._GCSFileSegReader(f"/{self.bucket_name}/{self.seg_prefix}", 0)

  def testInvalidMetadata(self):
      blob = self.bucket.blob(self.seg_prefix + "0")
      blob.metadata = {self.writer_cls._VALID_LENGTH: "10"}
      blob.upload_from_string("abc")
      blob.patch()

      with self.assertRaises(ValueError):
          gcs_file_seg_reader._GCSFileSegReader(f"/{self.bucket_name}/{self.seg_prefix}", 0)

  def ReadOneFileTest(self, read_size):
    blob = self.bucket.blob(self.seg_prefix + "0")
    
    # Upload the file with metadata
    blob.metadata = {self.writer_cls._VALID_LENGTH: "5"}
    blob.upload_from_string("1234567")
    blob.patch()

    r = gcs_file_seg_reader._GCSFileSegReader(f"/{self.bucket_name}/{self.seg_prefix}", 0)
    result = b""
    while True:
      tmp = r.read(read_size)
      if not tmp:
         break
      result += tmp
    self.assertEqual(b"12345", result)
    self.assertEqual(5, r.tell())

  def testReadBig(self):
    """Test read bigger than valid offset."""
    self.ReadOneFileTest(10)

  def testReadSmall(self):
    """Test read smaller than valid offset."""
    self.ReadOneFileTest(1)

  def testReadEqual(self):
    """Test read size equals valid offset."""
    self.ReadOneFileTest(5)

  def setUpMultipleFile(self):
      blob = self.bucket.blob(self.seg_prefix + "0")
      blob.metadata = {self.writer_cls._VALID_LENGTH: "5"}
      blob.upload_from_string("12345garbage")
  
      blob = self.bucket.blob(self.seg_prefix + "1")
      blob.metadata = {self.writer_cls._VALID_LENGTH: "5"}
      blob.upload_from_string("67890garbage")
  
      blob = self.bucket.blob(self.seg_prefix + "2")
      blob.metadata = {self.writer_cls._VALID_LENGTH: "6"}
      blob.upload_from_string("123456garbage")

  def testReadMultipleFiles(self):
    self.setUpMultipleFile()

    r = gcs_file_seg_reader._GCSFileSegReader(f"/{self.bucket_name}/{self.seg_prefix}", 2)
    result = b""
    while True:
      tmp = r.read(1)
      if not tmp:
        break
      result += tmp
    self.assertEqual(b"1234567890123456", result)
    self.assertEqual(len(result), r.tell())
