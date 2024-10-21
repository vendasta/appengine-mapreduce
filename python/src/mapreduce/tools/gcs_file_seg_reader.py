#!/usr/bin/env python
# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A simple reader for file segs produced by GCS output writer."""

from mapreduce import output_writers

from google.cloud import storage

# pylint: disable=protected-access
# pylint: disable=invalid-name

class _GCSFileSegReader:
  """A simple reader for file segs produced by GCS output writer.

  Internal use only.

  This reader conforms to Python stream interface.
  """

  def __init__(self, bucket, seg_prefix, last_seg_index):
    """Init.

    Instances are pickle safe.

    Args:
      seg_prefix: filename prefix for all segs. It is expected
        seg_prefix + index = seg filename.
      last_seg_index: the last index of all segs. int.
    """
    self._EOF = False
    self._offset = 0

    # fields related to seg.
    self._bucket = bucket
    self._seg_prefix = seg_prefix
    self._last_seg_index = last_seg_index
    self._seg_index = -1
    self._seg_valid_length = None
    self._seg = None
    self._next_seg()

  def read(self, n):
    """Read data from file segs.

    Args:
      n: max bytes to read. Must be positive.

    Returns:
      some bytes. May be smaller than n bytes. "" when no more data is left.
    """
    if self._EOF:
      return b""

    while self._seg_index <= self._last_seg_index:
      result = self._read_from_seg(n)
      if result != b"":
        return result
      else:
        self._next_seg()

    self._EOF = True
    return b""

  def close(self):
    if self._seg:
      self._seg.close()

  def tell(self):
    """Returns the next offset to read."""
    return self._offset

  def _next_seg(self):
    """Get next seg."""
    if self._seg:
      self._seg.close()
    self._seg_index += 1
    if self._seg_index > self._last_seg_index:
      self._seg = None
      return

    filename = self._seg_prefix + str(self._seg_index)
    blob = self._bucket.blob(filename)
    blob.reload()

    writer = output_writers._GoogleCloudStorageOutputWriter
    if blob.metadata is None or writer._VALID_LENGTH not in blob.metadata:
      raise ValueError("Expect %s in metadata for file %s." % (writer._VALID_LENGTH, filename))
    self._seg_valid_length = int(blob.metadata[writer._VALID_LENGTH])
    if self._seg_valid_length > blob.size:
      raise ValueError("Valid length %s is too big for file %s of length %s" % (self._seg_valid_length, filename, blob.size))
    self._seg = blob.open("rb")

  def _read_from_seg(self, n):
    """Read from current seg.

    Args:
      n: max number of bytes to read.

    Returns:
      valid bytes from the current seg. "" if no more is left.
    """
    result = self._seg.read(size=n)
    if result == b"":
      return result
    offset = self._seg.tell()
    if offset > self._seg_valid_length:
      extra = offset - self._seg_valid_length
      result = result[:-1*extra]
    self._offset += len(result)
    return result
