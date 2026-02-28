#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.




# pylint: disable=g-bad-name

import datetime

import pipeline
from google.cloud import ndb
from testlib import testutil

from mapreduce import (context, errors, input_readers, mapper_pipeline, model,
                       test_support)


class FakeEntity(ndb.Model):
  data = ndb.StringProperty()
  dt = ndb.DateTimeProperty(default=datetime.datetime(2000, 1, 1))


class FakeOutputEntity(ndb.Model):
  data = ndb.StringProperty()


class RetryCount(ndb.Model):
  retries = ndb.IntegerProperty()


def fake_fail_map(_):
  """Always fail job immediately."""
  raise errors.FailJobError()


def fake_slice_retry_map(entity):
  """Raise exception for 11 times when data is 100."""
  if entity.data == "100":
    retry_count = RetryCount.get_by_id(entity.data)
    if not retry_count:
      retry_count = RetryCount(id=entity.data, retries=0)
    if retry_count.retries < 11:
      retry_count.retries += 1
      retry_count.put()
      raise Exception()
  FakeOutputEntity(id=entity.data, data=entity.data).put()


def fake_shard_retry_map(entity):
  """Raise exception 12 times when data is 100."""
  if entity.data == "100":
    retry_count = RetryCount.get_by_id(entity.data)
    if not retry_count:
      retry_count = RetryCount(id=entity.data, retries=0)
    if retry_count.retries < 12:
      retry_count.retries += 1
      retry_count.put()
      raise Exception()
  FakeOutputEntity(id=entity.data, data=entity.data).put()


def fake_shard_retry_too_many_map(entity):
  """Raise shard retry exception 45 times when data is 100."""
  if entity.data == "100":
    retry_count = RetryCount.get_by_id(entity.data)
    if not retry_count:
      retry_count = RetryCount(id=entity.data, retries=0)
    if retry_count.retries < 45:
      retry_count.retries += 1
      retry_count.put()
      raise Exception()
  FakeOutputEntity(id=entity.data, data=entity.data).put()


def fake_map(entity):
  """Test map handler."""
  yield (entity.data, "")


# pylint: disable=unused-argument
def fake_empty_handler(entity):
  """Test handler that does nothing."""
  pass


class MapperPipelineTest(testutil.HandlerTestBase):
  """Tests for MapperPipeline."""

  def testEmptyMapper(self):
    """Test empty mapper over empty dataset."""
    p = mapper_pipeline.MapperPipeline(
        "empty_map",
        handler_spec=__name__ + ".fake_empty_handler",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "input_reader": {
                "entity_kind": __name__ + ".FakeEntity",
                "filters": [("dt", "=", datetime.datetime(2000, 1, 1))],
                },
            },
        )
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertFalse(p.was_aborted)

    p = mapper_pipeline.MapperPipeline.from_id(p.pipeline_id)
    counters = p.outputs.counters.value
    self.assertTrue(counters)
    self.assertTrue(context.COUNTER_MAPPER_WALLTIME_MS in counters)
    self.assertEqual([], p.outputs.default.value)
    self.assertTrue(p.outputs.job_id.filled)
    state = model.MapreduceState.get_by_job_id(p.outputs.job_id.value)
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS, state.result_status)
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     p.outputs.result_status.value)

  def testFailedMap(self):
    for i in range(1):
      FakeEntity(data=str(i)).put()

    pipeline.pipeline._DEFAULT_MAX_ATTEMPTS = 1

    p = mapper_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".fake_fail_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "input_reader": {
                "entity_kind": __name__ + "." + FakeEntity.__name__,
            },
        },
        shards=5)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = mapper_pipeline.MapperPipeline.from_id(p.pipeline_id)
    self.assertTrue(p.was_aborted)

    self.assertTrue(p.outputs.job_id.filled)
    state = model.MapreduceState.get_by_job_id(p.outputs.job_id.value)
    self.assertEqual(model.MapreduceState.RESULT_FAILED, state.result_status)
    self.assertFalse(p.outputs.result_status.filled)
    self.assertFalse(p.outputs.default.filled)

    self.assertTrue(p.was_aborted)

  def testProcessEntities(self):
    """Test empty mapper over non-empty dataset."""
    for _ in range(100):
      FakeEntity().put()

    p = mapper_pipeline.MapperPipeline(
        "empty_map",
        handler_spec=__name__ + ".fake_empty_handler",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "input_reader": {
                "entity_kind": __name__ + ".FakeEntity",
                },
            },
        )
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertFalse(p.was_aborted)

    p = mapper_pipeline.MapperPipeline.from_id(p.pipeline_id)

    self.assertTrue(p.outputs.job_id.filled)
    counters = p.outputs.counters.value
    self.assertTrue(counters)
    self.assertTrue(context.COUNTER_MAPPER_WALLTIME_MS in counters)
    self.assertEqual(100, counters[context.COUNTER_MAPPER_CALLS])
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     p.outputs.result_status.value)
    self.assertEqual([], p.outputs.default.value)

  def testSliceRetry(self):
    entity_count = 200
    ndb.delete_multi(FakeOutputEntity.query().fetch(keys_only=True))
    ndb.delete_multi(RetryCount.query().fetch(keys_only=True))

    for i in range(entity_count):
      FakeEntity(data=str(i)).put()

    p = mapper_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".fake_slice_retry_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "input_reader": {
                "entity_kind": __name__ + "." + FakeEntity.__name__,
            },
        },
        shards=5)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertFalse(p.was_aborted)

    p = mapper_pipeline.MapperPipeline.from_id(p.pipeline_id)
    outputs = []
    for output in FakeOutputEntity.query():
      outputs.append(int(output.data))
    outputs.sort()

    expected_outputs = [i for i in range(entity_count)]
    expected_outputs.sort()
    self.assertEqual(expected_outputs, outputs)

  def testShardRetry(self):
    entity_count = 200
    ndb.delete_multi(FakeOutputEntity.query().fetch(keys_only=True))
    ndb.delete_multi(RetryCount.query().fetch(keys_only=True))

    for i in range(entity_count):
      FakeEntity(data=str(i)).put()

    p = mapper_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".fake_shard_retry_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "input_reader": {
                "entity_kind": __name__ + "." + FakeEntity.__name__,
            },
        },
        shards=5)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertFalse(p.was_aborted)

    p = mapper_pipeline.MapperPipeline.from_id(p.pipeline_id)
    outputs = []
    for output in FakeOutputEntity.query():
      outputs.append(int(output.data))
    outputs.sort()

    expected_outputs = [i for i in range(entity_count)]
    expected_outputs.sort()
    self.assertEqual(expected_outputs, outputs)

  def testShardRetryTooMany(self):
    entity_count = 200
    ndb.delete_multi(FakeOutputEntity.query().fetch(keys_only=True))
    ndb.delete_multi(RetryCount.query().fetch(keys_only=True))

    for i in range(entity_count):
      FakeEntity(data=str(i)).put()

    p = mapper_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".fake_shard_retry_too_many_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "input_reader": {
                "entity_kind": __name__ + "." + FakeEntity.__name__,
            },
        },
        shards=5)
    p.max_attempts = 1
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    state = model.MapreduceState.query().get()
    self.assertEqual(model.MapreduceState.RESULT_FAILED, state.result_status)
