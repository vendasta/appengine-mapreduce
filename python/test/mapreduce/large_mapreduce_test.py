#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.

"""Testing large mapreduce jobs."""

from google.appengine.ext import db

from mapreduce import input_readers
from mapreduce import mapreduce_pipeline
from mapreduce import output_writers
from mapreduce import records
from mapreduce import test_support
from testlib import testutil


class FakeEntity(db.Model):
  """Test entity class."""
  data = db.TextProperty()


# pylint: disable=unused-argument
def map_yield_lots_of_values(entity):
  """Test map handler that yields lots of pairs."""
  for _ in range(50000):
    yield (1, " " * 100)


def reduce_length(key, values):
  """Reduce function yielding a string with key and values length."""
  yield str((key, len(values)))


class LargeMapreduceTest(testutil.CloudStorageTestBase, testutil.HandlerTestBase):
  """Large tests for MapperPipeline."""

  def testLotsOfValuesForSingleKey(self):
    FakeEntity(data=str(1)).put()
    # Run Mapreduce
    p = mapreduce_pipeline.MapreducePipeline(
        self.gcsPrefix,
        __name__ + ".map_yield_lots_of_values",
        __name__ + ".reduce_length",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=(
            output_writers.__name__ + ".GoogleCloudStorageRecordOutputWriter"),
        mapper_params={
            "bucket_name": self.TEST_BUCKET,
            "entity_kind": __name__ + "." + FakeEntity.__name__,
        },
        reducer_params={
            "output_writer": {
                "bucket_name": self.TEST_BUCKET
            },
        },
        shards=16)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertFalse(p.was_aborted)

    # Verify reduce output.
    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    output_data = []
    for output_file in p.outputs.default.value:
      with self.bucket.blob(output_file).open("rb") as f:
        for record in records.RecordsReader(f):
          output_data.append(record)

    expected_data = [b"('1', 50000)"]
    expected_data.sort()
    output_data.sort()
    self.assertEqual(expected_data, output_data)

