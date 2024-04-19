#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.

"""Testing large mapreduce jobs."""



# Using opensource naming conventions, pylint: disable=g-bad-name

import unittest


import pipeline
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


class LargeMapreduceTest(testutil.HandlerTestBase):
  """Large tests for MapperPipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testLotsOfValuesForSingleKey(self):
    FakeEntity(data=str(1)).put()
    # Run Mapreduce
    p = mapreduce_pipeline.MapreducePipeline(
        "test",
        __name__ + ".map_yield_lots_of_values",
        __name__ + ".reduce_length",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=(
            output_writers.__name__ + ".GoogleCloudStorageRecordOutputWriter"),
        mapper_params={
            "entity_kind": __name__ + "." + FakeEntity.__name__,
        },
        reducer_params={
            "output_writer": {
                "bucket_name": "test"
            },
        },
        shards=16)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEqual(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline successful:"))

    # Verify reduce output.
    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    output_data = []
    for output_file in p.outputs.default.value:
      with cloudstorage.open(output_file, "r") as f:
        for record in records.RecordsReader(f):
          output_data.append(record)

    expected_data = ["('1', 50000)"]
    expected_data.sort()
    output_data.sort()
    self.assertEqual(expected_data, output_data)



