#!/usr/bin/env python
"""Tests for gcs_file_seg_reader."""

# pylint: disable=g-bad-name


from mapreduce import output_writers
from mapreduce.tools import gcs_file_seg_reader


from testlib import testutil


class GCSFileSegReaderTest(testutil.CloudStorageTestBase):
  """Test GCSFileSegReader."""

  def setUp(self):
    super().setUp()
    self.writer_cls = output_writers._GoogleCloudStorageOutputWriter
    self.seg_prefix = f"{self.gcsPrefix}/prefix-"

  def testMissingMetadata(self):
    f = self.bucket.blob(self.seg_prefix + "0").open("w")
    f.write("abc")
    f.close()

    with self.assertRaises(ValueError):
        gcs_file_seg_reader._GCSFileSegReader(self.bucket, self.seg_prefix, 0)

  def testInvalidMetadata(self):
      blob = self.bucket.blob(self.seg_prefix + "0")
      blob.metadata = {self.writer_cls._VALID_LENGTH: "10"}
      blob.upload_from_string("abc")
      blob.patch()

      with self.assertRaises(ValueError):
          gcs_file_seg_reader._GCSFileSegReader(self.bucket, self.seg_prefix, 0)

  def ReadOneFileTest(self, read_size):
    blob = self.bucket.blob(self.seg_prefix + "0")
    
    # Upload the file with metadata
    blob.metadata = {self.writer_cls._VALID_LENGTH: "5"}
    blob.upload_from_string("1234567")
    blob.patch()

    r = gcs_file_seg_reader._GCSFileSegReader(self.bucket, self.seg_prefix, 0)
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

    r = gcs_file_seg_reader._GCSFileSegReader(self.bucket, self.seg_prefix, 2)
    result = b""
    while True:
      tmp = r.read(1)
      if not tmp:
        break
      result += tmp
    self.assertEqual(b"1234567890123456", result)
    self.assertEqual(len(result), r.tell())
