#!/usr/bin/env python

# Fix up paths for running tests.
import os
import sys


sys.path.append(os.path.join(os.path.dirname(__file__), "../../../../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../../../"))

from testlib import testutil

import mock
import unittest

from mapreduce.api.map_job import output_writer


class OutputWriterTest(unittest.TestCase):

  def testBeginEndSlice(self):
    writer = output_writer.OutputWriter()
    slice_ctx = mock.Mock()
    writer.begin_slice(slice_ctx)
    self.assertEqual(slice_ctx, writer._slice_ctx)
    writer.end_slice(slice_ctx)
    self.assertEqual(None, writer._slice_ctx)


if __name__ == '__main__':
  unittest.main()
