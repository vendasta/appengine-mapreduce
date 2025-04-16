#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.


# Using opensource naming conventions, pylint: disable=g-bad-name

from google.appengine.ext import db
from testlib import testutil

from mapreduce import (input_readers, mapreduce_pipeline, operation,
                       output_writers, shuffler, test_support)


class FakeEntity(db.Model):
  """Test entity class."""
  data = db.TextProperty()


def fake_combiner_map(entity):
  """Tests map handler for use with the Combiner test."""
  yield str(int(entity.data) % 4), entity.data


class FakeCombiner:
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


def fake_combiner_reduce(key, values):
  yield repr((key, sum([int(x) for x in values]))) + "\n"


class CombinerTest(testutil.CloudStorageTestBase, testutil.HandlerTestBase):
  """Tests for combiners."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    self.old_max_values_count = shuffler._MergePipeline._MAX_VALUES_COUNT
    shuffler._MergePipeline._MAX_VALUES_COUNT = 1
    self.addCleanup(FakeCombiner.reset)

  def tearDown(self):
    shuffler._MergePipeline._MAX_VALUES_COUNT = self.old_max_values_count
    testutil.HandlerTestBase.tearDown(self)

  def testNoCombiner(self):
    """Test running with low values count but without combiner."""
    entity_count = 50

    for i in range(entity_count):
      FakeEntity(data=str(i)).put()
      FakeEntity(data=str(i)).put()

    p = mapreduce_pipeline.MapreducePipeline(
        self.gcsPrefix,
        __name__ + ".fake_combiner_map",
        __name__ + ".fake_combiner_reduce",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
        output_writers.__name__ + ".GoogleCloudStorageOutputWriter",
        mapper_params={
          "bucket_name": self.TEST_BUCKET,
          "entity_kind": __name__ + ".FakeEntity",
        },
        reducer_params={
            "output_writer": {
                "bucket_name": self.TEST_BUCKET
            },
        },
        shards=2)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    self.assertEqual(2, len(p.outputs.default.value))
    file_content = []
    for input_file in p.outputs.default.value:
      blob = self.bucket.blob(input_file)
      with blob.open("rb") as infile:
        for line in infile:
          file_content.append(line.strip())

    file_content = sorted(file_content)
    self.assertEqual(
        [b"('0', 624)", b"('1', 650)", b"('2', 576)", b"('3', 600)"],
        file_content)

  def testCombiner(self):
    """Test running with low values count but with combiner."""
    entity_count = 50

    for i in range(entity_count):
      FakeEntity(data=str(i)).put()
      FakeEntity(data=str(i)).put()

    p = mapreduce_pipeline.MapreducePipeline(
        self.gcsPrefix,
        __name__ + ".fake_combiner_map",
        __name__ + ".fake_combiner_reduce",
        combiner_spec=__name__ + ".FakeCombiner",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
        output_writers.__name__ + ".GoogleCloudStorageOutputWriter",
        mapper_params={
          "bucket_name": self.TEST_BUCKET,
          "entity_kind": __name__ + ".FakeEntity",
        },
        reducer_params={
            "output_writer": {
                "bucket_name": self.TEST_BUCKET
            },
        },
        shards=2)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    self.assertEqual(2, len(p.outputs.default.value))
    file_content = []
    for input_file in p.outputs.default.value:
      blob = self.bucket.blob(input_file)
      with blob.open("rb") as infile:
        for line in infile:
          file_content.append(line.strip())

    file_content = sorted(file_content)
    self.assertEqual(
        [b"('0', 624)", b"('1', 650)", b"('2', 576)", b"('3', 600)"],
        file_content)

    self.assertTrue(FakeCombiner.invocations)

    for invocation in FakeCombiner.invocations:
      key = invocation[0]
      values = invocation[1]
      self.assertTrue(key)
      self.assertTrue(values)
      self.assertEqual(1, len(values))
      self.assertTrue(int(values[0]) % 4 == int(key))



