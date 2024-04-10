#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.




# Using opensource naming conventions, pylint: disable=g-bad-name

import os
import sys
import unittest

import pipeline
from google.appengine.ext import db

# Fix up paths for running tests.
sys.path.append(os.path.join(os.path.dirname(__file__), "../../src"))

from mapreduce import input_readers
from mapreduce import mapreduce_pipeline
from mapreduce import operation
from mapreduce import output_writers
from mapreduce import shuffler
from mapreduce import test_support
from testlib import testutil


class TestEntity(db.Model):
  """Test entity class."""
  data = db.TextProperty()


def test_combiner_map(entity):
  """Tests map handler for use with the Combiner test."""
  yield str(int(entity.data) % 4), entity.data


class TestCombiner(object):
  """Test combine handler."""
  invocations = []

  def __call__(self, key, values, combiner_values):
    self.invocations.append((key, values, combiner_values))

    value_ints = [int(x) for x in values]
    combiner_values_int = [int(x) for x in combiner_values]
    yield sum(value_ints + combiner_values_int)
    yield operation.counters.Increment("combiner-call")

  @classmethod
  def reset(cls):
    cls.invocations = []


def test_combiner_reduce(key, values):
  yield repr((key, sum([int(x) for x in values]))) + "\n"


class CombinerTest(testutil.HandlerTestBase):
  """Tests for combiners."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

    self.old_max_values_count = shuffler._MergePipeline._MAX_VALUES_COUNT
    shuffler._MergePipeline._MAX_VALUES_COUNT = 1

    TestCombiner.reset()

  def tearDown(self):
    shuffler._MergePipeline._MAX_VALUES_COUNT = self.old_max_values_count
    testutil.HandlerTestBase.tearDown(self)

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testNoCombiner(self):
    """Test running with low values count but without combiner."""
    # Even though this test doesn't have combiner specified, it's still
    # interesting to run. It forces MergePipeline to produce partial
    # key values and we verify that they are combined correctly in reader.

    # Prepare test data
    entity_count = 200

    for i in range(entity_count):
      TestEntity(data=str(i)).put()
      TestEntity(data=str(i)).put()

    p = mapreduce_pipeline.MapreducePipeline(
        "test",
        __name__ + ".test_combiner_map",
        __name__ + ".test_combiner_reduce",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
        output_writers.__name__ + ".GoogleCloudStorageOutputWriter",
        mapper_params={
            "entity_kind": __name__ + ".TestEntity",
            },
        reducer_params={
            "output_writer": {
                "bucket_name": "testbucket"
            },
        },
        shards=4)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    self.assertEqual(4, len(p.outputs.default.value))
    file_content = []
    for input_file in p.outputs.default.value:
      with cloudstorage.open(input_file) as infile:
        for line in infile:
          file_content.append(line.strip())

    file_content = sorted(file_content)

    self.assertEqual(
        ["('0', 9800)", "('1', 9900)", "('2', 10000)", "('3', 10100)"],
        file_content)

  def testCombiner(self):
    """Test running with low values count but with combiner."""
    # Prepare test data
    entity_count = 200

    for i in range(entity_count):
      TestEntity(data=str(i)).put()
      TestEntity(data=str(i)).put()

    p = mapreduce_pipeline.MapreducePipeline(
        "test",
        __name__ + ".test_combiner_map",
        __name__ + ".test_combiner_reduce",
        combiner_spec=__name__ + ".TestCombiner",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
        output_writers.__name__ + ".GoogleCloudStorageOutputWriter",
        mapper_params={
            "entity_kind": __name__ + ".TestEntity",
        },
        reducer_params={
            "output_writer": {
                "bucket_name": "testbucket"
            },
        },
        shards=4)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    self.assertEqual(4, len(p.outputs.default.value))
    file_content = []
    for input_file in p.outputs.default.value:
      with cloudstorage.open(input_file) as infile:
        for line in infile:
          file_content.append(line.strip())

    file_content = sorted(file_content)

    self.assertEqual(
        ["('0', 9800)", "('1', 9900)", "('2', 10000)", "('3', 10100)"],
        file_content)

    self.assertTrue(TestCombiner.invocations)

    for invocation in TestCombiner.invocations:
      key = invocation[0]
      values = invocation[1]
      self.assertTrue(key)
      self.assertTrue(values)
      self.assertEqual(1, len(values))
      self.assertTrue(int(values[0]) % 4 == int(key))


if __name__ == "__main__":
  unittest.main()

