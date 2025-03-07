#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
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




# pylint: disable=g-bad-name
# pylint: disable=unused-argument


# os_compat must be first to ensure timezones are UTC.
# pylint: disable=unused-import
# pylint: disable=g-bad-import-order
from google.appengine.tools import os_compat
import werkzeug
import werkzeug.exceptions
# testutil must be imported before mock.
# pylint: disable=g-bad-import-order
from testlib import testutil

import base64
import collections
import datetime
import math
import os
import time
import json

from unittest import mock

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore
from google.appengine.api import datastore_errors
from google.appengine.api import taskqueue
from google.appengine.ext import db
from google.appengine.ext import key_range
from mapreduce import context
from mapreduce import control
from mapreduce import datastore_range_iterators as db_iters
from mapreduce import errors
from mapreduce import handlers
from mapreduce import hooks
from mapreduce import input_readers
from mapreduce import key_ranges
from mapreduce import map_job_context
from mapreduce import model
from mapreduce import operation
from mapreduce import output_writers
from mapreduce import parameters
from mapreduce import shard_life_cycle
from mapreduce import test_support
from mapreduce import util
from mapreduce.api import map_job


MAPPER_PARAMS = {"batch_size": 50}
PARAM_DONE_CALLBACK = model.MapreduceSpec.PARAM_DONE_CALLBACK
PARAM_DONE_CALLBACK_QUEUE = model.MapreduceSpec.PARAM_DONE_CALLBACK_QUEUE


class TestHooks(hooks.Hooks):
  """Test hooks class."""

  enqueue_worker_task_calls = []
  enqueue_done_task_calls = []
  enqueue_controller_task_calls = []
  enqueue_kickoff_task_calls = []

  def __init__(self, mapper):
    super().__init__(mapper)

  def enqueue_worker_task(self, task, queue_name):
    self.enqueue_worker_task_calls.append((task, queue_name))
    task.add(queue_name=queue_name)

  def enqueue_kickoff_task(self, task, queue_name):
    self.enqueue_kickoff_task_calls.append((task, queue_name))
    task.add(queue_name=queue_name)

  def enqueue_done_task(self, task, queue_name):
    self.enqueue_done_task_calls.append((task, queue_name))
    task.add(queue_name=queue_name)

  def enqueue_controller_task(self, task, queue_name):
    self.enqueue_controller_task_calls.append((task, queue_name))
    task.add(queue_name=queue_name)

  @classmethod
  def reset(cls):
    cls.enqueue_worker_task_calls = []
    cls.enqueue_done_task_calls = []
    cls.enqueue_controller_task_calls = []
    cls.enqueue_kickoff_task_calls = []


class TestKind(db.Model):
  """Used for testing."""

  foobar = db.StringProperty(default="meep")


def TestMap(entity):
  """Used for testing."""
  pass


class MockTime:
  """Simple class to use for mocking time() function."""

  now = time.time()

  @staticmethod
  def time():
    """Get current mock time."""
    return MockTime.now

  @staticmethod
  def advance_time(delta):
    """Advance current mock time by delta."""
    MockTime.now += delta


class TestEntity(db.Model):
  """Test entity class."""

  a = db.IntegerProperty(default=1)


class TestHandler:
  """Test handler which stores all processed entities keys.

  Properties:
    processed_keys: all keys of processed entities.
    delay: advances mock time by this delay on every call.
  """

  processed_keys = []
  delay = 0

  def __call__(self, entity):
    """Main handler process function.

    Args:
      entity: entity to process.
    """
    TestHandler.processed_keys.append(str(entity.key()))
    MockTime.advance_time(TestHandler.delay)

  @staticmethod
  def reset():
    """Clear processed_keys & reset delay to 0."""
    TestHandler.processed_keys = []
    TestHandler.delay = 0


class TestOperation(operation.Operation):
  """Test operation which records entity on execution."""

  processed_keys = []

  def __init__(self, entity):
    self.entity = entity

  def __call__(self, cxt):
    TestOperation.processed_keys.append(str(self.entity.key()))

  @classmethod
  def reset(cls):
    cls.processed_keys = []


def fake_handler_raise_exception(entity):
  """Test handler function which always raises exception.

  Raises:
    ValueError: always
  """
  raise ValueError()


def fake_handler_raise_fail_job_exception(entity):
  """Test handler function which always raises exception.

  Raises:
    FailJobError: always.
  """
  raise errors.FailJobError()


def fake_handler_yield_op(entity):
  """Test handler function which yields test operation twice for entity."""
  yield TestOperation(entity)
  yield TestOperation(entity)


def fake_param_validator_success(params):
  """Test parameter validator that is successful."""
  params["test"] = "good"


def fake_param_validator_raise_exception(params):
  """Test parameter validator that fails."""
  raise Exception("These params are bad")


def fake_handler_yield_keys(entity):
  """Test handler which yeilds entity keys."""
  yield entity.key()


class InputReader(input_readers.DatastoreInputReader):
  """Test input reader which records number of yields."""

  yields = 0
  # Used to uniquely identity an input reader instance across serializations.
  next_instance_id = 0

  def __init__(self, iterator, instance_id=None):
    super().__init__(iterator)
    self.instance_id = instance_id

  def __iter__(self):
    for entity in input_readers.DatastoreInputReader.__iter__(self):
      InputReader.yields += 1
      yield entity

  @classmethod
  def split_input(cls, mapper_spec):
    """Split into the exact number of shards asked for."""
    shard_count = mapper_spec.shard_count
    query_spec = cls._get_query_spec(mapper_spec)

    k_ranges = [key_ranges.KeyRangesFactory.create_from_list(
        [key_range.KeyRange()]) for _ in range(shard_count)]
    iters = [db_iters.RangeIteratorFactory.create_key_ranges_iterator(
        r, query_spec, cls._KEY_RANGE_ITER_CLS) for r in k_ranges]

    results = []
    for i in iters:
      results.append(cls(i, cls.next_instance_id))
      cls.next_instance_id += 1
    return results

  def to_json(self):
    return {"iter": self._iter.to_json(),
            "instance_id": self.instance_id}

  @classmethod
  def from_json(cls, json):
    """Create new DatastoreInputReader from json, encoded by to_json.

    Args:
      json: json representation of DatastoreInputReader.

    Returns:
      an instance of DatastoreInputReader with all data deserialized from json.
    """
    return cls(db_iters.RangeIteratorFactory.from_json(json["iter"]),
               json["instance_id"])

  @classmethod
  def reset(cls):
    cls.yields = 0
    cls.next_instance_id = 0


class EmptyInputReader(input_readers.DatastoreInputReader):
  """Always returns nothing from input splits."""

  @classmethod
  def split_input(cls, mapper_spec):
    return None


class TestOutputWriter(output_writers.OutputWriter):
  """Test output writer."""

  # store lifecycle events.
  events = []

  @classmethod
  def reset(cls):
    cls.events = []

  @classmethod
  def validate(cls, mapper_spec):
    assert isinstance(mapper_spec, model.MapperSpec)
    if "fail_writer_validate" in mapper_spec.params:
      raise Exception("Failed Validation")

  @classmethod
  def init_job(cls, mapreduce_state):
    assert isinstance(mapreduce_state, model.MapreduceState)
    cls.events.append("init_job")

  @classmethod
  def finalize_job(cls, mapreduce_state):
    assert isinstance(mapreduce_state, model.MapreduceState)
    cls.events.append("finalize_job")

  @classmethod
  def create(cls, mr_spec, shard_number, shard_attempt, _writer_state=None):
    cls.events.append("create-" + str(shard_number))
    return cls()

  def to_json(self):
    return {}

  @classmethod
  def from_json(cls, json_dict):
    return cls()

  def write(self, data):
    ctx = context.get()
    assert isinstance(ctx, context.Context)
    self.events.append("write-" + str(data))

  def finalize(self, ctx, shard_state):
    assert isinstance(ctx, context.Context)
    self.events.append("finalize-" + str(shard_state.shard_number))

  def _supports_slice_recovery(self, mapper_spec):
    return True

  def _recover(self, mr_spec, shard_number, shard_attempt):
    self.events.append("recover")
    return self.__class__()


class ShardLifeCycleOutputWriter(shard_life_cycle._ShardLifeCycle,
                                 TestOutputWriter):
  """OutputWriter implementing life cycle methods."""

  def begin_shard(self, shard_ctx):
    assert isinstance(shard_ctx, map_job_context.ShardContext)
    self.events.append("begin_shard-%s" % shard_ctx.id)

  def end_shard(self, shard_ctx):
    assert isinstance(shard_ctx, map_job_context.ShardContext)
    self.events.append("end_shard-%s" % shard_ctx.id)

  def begin_slice(self, slice_ctx):
    assert isinstance(slice_ctx, map_job_context.SliceContext)
    self.events.append("begin_slice-%s" % slice_ctx.number)

  def end_slice(self, slice_ctx):
    assert isinstance(slice_ctx, map_job_context.SliceContext)
    self.events.append("end_slice-%s" % slice_ctx.number)


class UnfinalizableTestOutputWriter(TestOutputWriter):
  """An output writer where all calls to finalize fail."""

  def finalize(self, ctx, shard_state):
    raise Exception("This will always break")


ENTITY_KIND = f"{TestEntity.__module__}.TestEntity"
MAPPER_HANDLER_SPEC = __name__ + "." + TestHandler.__name__

COUNTER_MAPPER_CALLS = context.COUNTER_MAPPER_CALLS


class MapreduceHandlerTestBase(testutil.HandlerTestBase):
  """Base class for all mapreduce's HugeTaskHandler tests.

  Contains common fixture and utility methods.
  """

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    TestHandler.reset()
    TestOutputWriter.reset()
    TestHooks.reset()

  def find_task_by_name(self, tasks, name):
    """Find a task with given name.

    Args:
      tasks: iterable of tasks.
      name: a name to look for.

    Returns:
      task or None
    """
    for task in tasks:
      if task["name"] == name:
        return task
    return None

  def verify_shard_task(self, task, shard_id, slice_id=0, eta=None,
                        countdown=None, verify_spec=True, **kwargs):
    """Checks that all shard task properties have expected values.

    Args:
      task: task to check.
      shard_id: expected shard id.
      slice_id: expected slice_id.
      eta: expected task eta.
      countdown: expected task delay from now.
      verify_spec: check mapreduce_spec if True.
      **kwargs: Extra keyword arguments to pass to verify_mapreduce_spec.
    """
    expected_task_name = handlers.MapperWorkerCallbackHandler.get_task_name(
        shard_id, slice_id)
    self.assertEqual(expected_task_name, task["name"])
    self.assertEqual("POST", task["method"])
    self.assertEqual("/mapreduce/worker_callback/" + shard_id, task["url"])
    if eta:
      self.assertEqual(eta.strftime("%Y/%m/%d %H:%M:%S"), task["eta"])
    if countdown:
      expected_etc_sec = time.time() + countdown
      eta_sec = time.mktime(time.strptime(task["eta"], "%Y/%m/%d %H:%M:%S"))
      self.assertTrue(expected_etc_sec < eta_sec + 10)

    with self.app.test_request_context(
      headers=dict(task["headers"]),
      data=base64.b64decode(task["body"]),
    ):
      from flask import request
      payload = model.HugeTask.decode_payload(request)
    self.assertEqual(str(shard_id), payload["shard_id"])
    self.assertEqual(str(slice_id), payload["slice_id"])

    if verify_spec:
      self.assertTrue(payload["mapreduce_spec"])
      mapreduce_spec = model.MapreduceSpec.from_json_str(
          payload["mapreduce_spec"])
      self.verify_mapreduce_spec(mapreduce_spec, **kwargs)

  def verify_mapreduce_spec(self, mapreduce_spec, **kwargs):
    """Check all mapreduce spec properties to have expected values.

    Args:
      mapreduce_spec: mapreduce spec to check as MapreduceSpec.
      kwargs: expected property values. Checks for default property value if
        particular property is not specified.
    """
    self.assertTrue(mapreduce_spec)
    self.assertEqual(kwargs.get("mapper_handler_spec", MAPPER_HANDLER_SPEC),
                      mapreduce_spec.mapper.handler_spec)
    self.assertEqual(kwargs.get("output_writer_spec", None),
                      mapreduce_spec.mapper.output_writer_spec)
    self.assertEqual(
        ENTITY_KIND,
        mapreduce_spec.mapper.params["input_reader"]["entity_kind"])
    self.assertEqual(kwargs.get("shard_count", 8),
                      mapreduce_spec.mapper.shard_count)
    self.assertEqual(kwargs.get("hooks_class_name"),
                      mapreduce_spec.hooks_class_name)

  def verify_shard_state(self, shard_state, **kwargs):
    """Checks that all shard state properties have expected values.

    Args:
      shard_state: shard state to check.
      kwargs: expected property values. Checks for default property value if
        particular property is not specified.
    """
    self.assertTrue(shard_state)

    self.assertEqual(kwargs.get("active", True), shard_state.active)
    self.assertEqual(kwargs.get("processed", 0),
                     shard_state.counters_map.get(COUNTER_MAPPER_CALLS))
    self.assertEqual(kwargs.get("result_status", None),
                     shard_state.result_status)
    self.assertEqual(kwargs.get("slice_retries", 0),
                     shard_state.slice_retries)
    self.assertEqual(kwargs.get("retries", 0),
                     shard_state.retries)
    self.assertEqual(kwargs.get("input_finished", False),
                     shard_state.input_finished)

  def verify_mapreduce_state(self, mapreduce_state, **kwargs):
    """Checks mapreduce state to have expected property values.

    Args:
      mapreduce_state: mapreduce state to check.
      kwargs: expected property values. Checks for default property value if
        particular property is not specified.
    """
    self.assertTrue(mapreduce_state)

    self.assertEqual(kwargs.get("active", True), mapreduce_state.active)
    self.assertEqual(kwargs.get("processed", 0),
                      mapreduce_state.counters_map.get(COUNTER_MAPPER_CALLS))
    self.assertEqual(kwargs.get("result_status", None),
                      mapreduce_state.result_status)

    mapreduce_spec = mapreduce_state.mapreduce_spec
    self.verify_mapreduce_spec(mapreduce_spec, **kwargs)

  def verify_controller_task(self, task, **kwargs):
    """Checks that all update status task properties have expected values.

    Args:
      task: task to check.
      kwargs: expected property values. Checks for default if property is not
        specified.
    """
    self.assertEqual("POST", task["method"])

    with self.app.test_request_context(
      headers=task["headers"],
      data=base64.b64decode(task["body"])
    ):
      from flask import request
      payload = model.HugeTask.decode_payload(request)
    mapreduce_spec = model.MapreduceSpec.from_json_str(
        payload["mapreduce_spec"])
    self.verify_mapreduce_spec(mapreduce_spec, **kwargs)
    self.assertEqual(
        "/mapreduce/controller_callback/" + mapreduce_spec.mapreduce_id,
        task["url"])

  def create_mapreduce_spec(self,
                            mapreduce_id,
                            shard_count=8,
                            mapper_handler_spec=MAPPER_HANDLER_SPEC,
                            mapper_parameters=None,
                            hooks_class_name=None,
                            output_writer_spec=None,
                            input_reader_spec=None):
    """Create a new valid mapreduce_spec.

    Args:
      mapreduce_id: mapreduce id.
      shard_count: number of shards in the handlers.
      mapper_handler_spec: handler specification to use for handlers.
      hooks_class_name: fully qualified name of the hooks class.

    Returns:
      new MapreduceSpec.
    """
    params = {
        "input_reader": {
            "entity_kind": __name__ + "." + TestEntity.__name__
        },
    }
    if mapper_parameters is not None:
      params.update(mapper_parameters)

    mapper_spec = model.MapperSpec(
        mapper_handler_spec,
        input_reader_spec or __name__ + ".InputReader",
        params,
        shard_count,
        output_writer_spec=output_writer_spec)
    mapreduce_spec = model.MapreduceSpec(
        "my job", mapreduce_id, mapper_spec.to_json(),
        params=map_job.JobConfig._get_default_mr_params(),
        hooks_class_name=hooks_class_name)

    self.verify_mapreduce_spec(mapreduce_spec,
                               shard_count=shard_count,
                               mapper_handler_spec=mapper_handler_spec,
                               hooks_class_name=hooks_class_name,
                               output_writer_spec=output_writer_spec)

    state = model.MapreduceState(
        key_name=mapreduce_id,
        last_poll_time=datetime.datetime.now())
    state.mapreduce_spec = mapreduce_spec
    state.active = True
    state.shard_count = shard_count
    state.active_shards = shard_count
    state.put()

    return mapreduce_spec

  def create_shard_state(self, mapreduce_id, shard_number):
    """Creates a new valid shard state.

    Args:
      mapreduce_id: mapreduce id to create state for as string.
      shard_number: shard number as int.

    Returns:
      new ShardState.
    """
    shard_state = model.ShardState.create_new(mapreduce_id, shard_number)
    self.verify_shard_state(shard_state)
    return shard_state

  def create_and_store_shard_state(self, mapreduce_id, shard_number):
    """Creates a new valid shard state and saves it into memcache.

    Args:
      mapreduce_id: mapreduce id to create state for as string.
      shard_number: shard number as int.

    Returns:
      new ShardState.
    """
    shard_state = self.create_shard_state(mapreduce_id, shard_number)
    shard_state.put()
    return shard_state

  def key(self, entity_id):
    """Create a key for TestEntity with specified id.

    Used to shorted expected data.

    Args:
      entity_id: entity id
    Returns:
      db.Key instance with specified id for TestEntity.
    """
    return db.Key.from_path("TestEntity", entity_id)


class StartJobHandlerTest(testutil.HandlerTestBase):
  """Test handlers.StartJobHandler.

  This class mostly tests request handling and parameter parsing aspect of the
  handler.
  """

  NAME = "my_job"
  HANDLER_SPEC = MAPPER_HANDLER_SPEC
  ENTITY_KIND = __name__ + "." + TestEntity.__name__
  INPUT_READER_SPEC = ("mapreduce.input_readers."
                       "DatastoreInputReader")
  OUTPUT_WRITER_SPEC = __name__ + ".TestOutputWriter"
  SHARD_COUNT = "9"
  PROCESSING_RATE = "1234"
  QUEUE = "crazy-queue"

  def setUp(self):
    """Sets up the test harness."""
    super().setUp()
    self.handler = handlers.StartJobHandler()

  def testCSRF(self):
    """Tests that that handler only accepts AJAX requests."""
    with self.app.test_request_context(method="POST"), self.assertRaises(werkzeug.exceptions.Forbidden):
      self.handler.dispatch_request()

  def testSmoke(self):
    """Verifies main execution path of starting scan over several entities."""
    for _ in range(100):
      TestEntity().put()
    with self.app.test_request_context(
      method="POST",
      data={
        "name": self.NAME,
        "mapper_handler": self.HANDLER_SPEC,
        "mapper_input_reader": self.INPUT_READER_SPEC,
        "mapper_output_writer": self.OUTPUT_WRITER_SPEC,
        "mapper_params.shard_count": self.SHARD_COUNT,
        "mapper_params.entity_kind": self.ENTITY_KIND,
        "mapper_params.processing_rate": self.PROCESSING_RATE,
        "mapper_params.queue_name": self.QUEUE
      },
      headers={"X-Requested-With": "XMLHttpRequest"}
    ):
      self.handler.dispatch_request()

    tasks = self.taskqueue.GetTasks(self.QUEUE)
    # Only kickoff task should be there.
    self.assertEqual(1, len(tasks))
    task_mr_id = test_support.decode_task_payload(tasks[0]).get("mapreduce_id")

    # Verify mr id is generated.
    mapreduce_id = self.handler.json_response["mapreduce_id"]
    self.assertTrue(mapreduce_id)
    self.assertEqual(task_mr_id, mapreduce_id)

    # Verify state is created.
    state = model.MapreduceState.get_by_job_id(
        self.handler.json_response["mapreduce_id"])
    self.assertTrue(state)
    self.assertTrue(state.active)
    self.assertEqual(0, state.active_shards)

    # Verify mapreduce spec.
    self.assertEqual(self.HANDLER_SPEC,
                     state.mapreduce_spec.mapper.handler_spec)
    self.assertEqual(self.NAME, state.mapreduce_spec.name)
    self.assertEqual(self.INPUT_READER_SPEC,
                     state.mapreduce_spec.mapper.input_reader_spec)
    self.assertEqual(self.OUTPUT_WRITER_SPEC,
                     state.mapreduce_spec.mapper.output_writer_spec)
    self.assertEqual(int(self.SHARD_COUNT),
                     state.mapreduce_spec.mapper.shard_count)
    self.assertEqual(int(self.PROCESSING_RATE),
                     state.mapreduce_spec.mapper.params["processing_rate"])
    self.assertEqual(self.QUEUE,
                     state.mapreduce_spec.params["queue_name"])

  def testOtherApp(self):
    """Verifies main execution path of starting scan over several entities."""
    apiproxy_stub_map.apiproxy.GetStub("datastore_v3").SetTrusted(True)
    TestEntity(_app="otherapp").put()
    
    with self.app.test_request_context(
      method="POST",
      data={
        "name": self.NAME,
        "mapper_handler": self.HANDLER_SPEC,
        "mapper_input_reader": self.INPUT_READER_SPEC,
        "mapper_output_writer": self.OUTPUT_WRITER_SPEC,
        "mapper_params.shard_count": self.SHARD_COUNT,
        "mapper_params.entity_kind": self.ENTITY_KIND,
        "mapper_params.processing_rate": self.PROCESSING_RATE,
        "mapper_params.queue_name": self.QUEUE,
        "mapper_params._app": "otherapp"
      },
      headers={"X-Requested-With": "XMLHttpRequest"}
    ):
      self.handler.dispatch_request()

    # Verify state is created.
    state = model.MapreduceState.get_by_job_id(
        self.handler.json_response["mapreduce_id"])
    self.assertEqual("otherapp", state.app_id)

  def testRequiredParams(self):
    """Tests that required parameters are enforced."""
    TestEntity().put()

    data={
      "name": self.NAME,
      "mapper_handler": self.HANDLER_SPEC,
      "mapper_input_reader": self.INPUT_READER_SPEC,
      "mapper_output_writer": self.OUTPUT_WRITER_SPEC,
      "mapper_params.entity_kind": self.ENTITY_KIND
    }

    with self.app.test_request_context(
      method="POST",
      headers={"X-Requested-With": "XMLHttpRequest"},
      data=data
    ):
      self.handler.handle()

    data.pop("name")
    with self.app.test_request_context(
      method="POST",
      headers={"X-Requested-With": "XMLHttpRequest"},
      data=data
    ), self.assertRaises(errors.NotEnoughArgumentsError):
      self.handler.handle()

    data["name"] = self.NAME
    data.pop("mapper_input_reader")
    with self.app.test_request_context(
      method="POST",
      headers={"X-Requested-With": "XMLHttpRequest"},
      data=data
    ), self.assertRaises(errors.NotEnoughArgumentsError):
      self.handler.handle()

    data["mapper_input_reader"] = self.INPUT_READER_SPEC
    data.pop("mapper_handler")
    with self.app.test_request_context(
      method="POST",
      headers={"X-Requested-With": "XMLHttpRequest"},
      data=data
    ), self.assertRaises(errors.NotEnoughArgumentsError):
      self.handler.handle()

    data["mapper_handler"] = self.HANDLER_SPEC
    data.pop("mapper_params.entity_kind")
    with self.app.test_request_context(
      method="POST",
      headers={"X-Requested-With": "XMLHttpRequest"},
      data=data
    ), self.assertRaises(input_readers.BadReaderParamsError):
      self.handler.handle()

    data["mapper_params.entity_kind"] = self.ENTITY_KIND
    with self.app.test_request_context(
      method="POST",
      headers={"X-Requested-With": "XMLHttpRequest"},
      data=data
    ):
      self.handler.handle()

  def testParameterValidationSuccess(self):
    """Tests validating user-supplied parameters."""
    TestEntity().put()

    with self.app.test_request_context(
      method="POST",
      headers={"X-Requested-With": "XMLHttpRequest"},
      data={
        "name": self.NAME,
        "mapper_handler": self.HANDLER_SPEC,
        "mapper_input_reader": self.INPUT_READER_SPEC,
        "mapper_output_writer": self.OUTPUT_WRITER_SPEC,
        "mapper_params.entity_kind": self.ENTITY_KIND,
        "mapper_params.one": ["red", "blue"],
        "mapper_params.two": "green",
        "mapper_params_validator": __name__ + ".fake_param_validator_success"
      }
    ):
      self.handler.post()

    state = model.MapreduceState.get_by_job_id(
        self.handler.json_response["mapreduce_id"])
    params = state.mapreduce_spec.mapper.params

    self.assertEqual(["red", "blue"], params["one"])
    self.assertEqual("green", params["two"])

    # From the validator function
    self.assertEqual("good", params["test"])

  def testMapreduceParameters(self):
    """Tests propagation of user-supplied mapreduce parameters."""
    TestEntity().put()

    with self.app.test_request_context(
      method="POST",
      headers={"X-Requested-With": "XMLHttpRequest"},
      data={
        "name": self.NAME,
        "mapper_handler": self.HANDLER_SPEC,
        "mapper_input_reader": self.INPUT_READER_SPEC,
        "mapper_output_writer": self.OUTPUT_WRITER_SPEC,
        "mapper_params.entity_kind": self.ENTITY_KIND,
        "params.one": ["red", "blue"],
        "params.two": "green",
        "params_validator": __name__ + ".fake_param_validator_success"
      }
    ):
      self.handler.post()

    state = model.MapreduceState.get_by_job_id(
        self.handler.json_response["mapreduce_id"])
    params = state.mapreduce_spec.params
    self.assertEqual(["red", "blue"], params["one"])
    self.assertEqual("green", params["two"])

    # From the validator function
    self.assertEqual("good", params["test"])

  def testParameterValidationFailure(self):
    """Tests when validating user-supplied parameters fails."""
    try:
      with self.app.test_request_context(
        method="POST",
        headers={"X-Requested-With": "XMLHttpRequest"},
        data={
          "name": self.NAME,
          "mapper_handler": self.HANDLER_SPEC,
          "mapper_input_reader": self.INPUT_READER_SPEC,
          "mapper_output_writer": self.OUTPUT_WRITER_SPEC,
          "mapper_params.entity_kind": self.ENTITY_KIND,
          "mapper_params_validator": __name__ + ".fake_param_validator_raise_exception"
        }
      ):
        self.handler.handle()
        self.fail()
    except Exception as e:
      self.assertEqual("These params are bad", str(e))

  def testParameterValidationUnknown(self):
    """Tests the user-supplied parameter validation function cannot be found."""
    with self.app.test_request_context(
      method="POST",
      headers={"X-Requested-With": "XMLHttpRequest"},
      data={
        "name": self.NAME,
        "mapper_handler": self.HANDLER_SPEC,
        "mapper_input_reader": self.INPUT_READER_SPEC,
        "mapper_output_writer": self.OUTPUT_WRITER_SPEC,
        "mapper_params.entity_kind": self.ENTITY_KIND,
        "mapper_params_validator": "does_not_exist"
      }
    ), self.assertRaises(ImportError):
      self.handler.handle()

  def testOutputWriterValidateFails(self):
    TestEntity().put()

    with self.app.test_request_context(
      method="POST",
      headers={"X-Requested-With": "XMLHttpRequest"},
      data={
        "name": self.NAME,
        "mapper_handler": self.HANDLER_SPEC,
        "mapper_input_reader": self.INPUT_READER_SPEC,
        "mapper_output_writer": self.OUTPUT_WRITER_SPEC,
        "mapper_params.entity_kind": self.ENTITY_KIND,
        "mapper_output_writer": __name__ + ".TestOutputWriter",
        "mapper_params.fail_writer_validate": "true"
      }
    ), self.assertRaises(Exception):
      self.handler.handle()


class StartJobHandlerFunctionalTest(testutil.HandlerTestBase):
  """Test _start_map function.

  Since this function is often called separately from the handler as well as
  by the handler directly.
  """

  NAME = "my_job"
  HANLDER_SPEC = MAPPER_HANDLER_SPEC
  ENTITY_KIND = __name__ + "." + TestEntity.__name__
  INPUT_READER_SPEC = ("mapreduce.input_readers."
                       "DatastoreInputReader")
  OUTPUT_WRITER_SPEC = __name__ + ".TestOutputWriter"
  SHARD_COUNT = "9"
  QUEUE = "crazy-queue"
  MAPREDUCE_SPEC_PARAMS = map_job.JobConfig._get_default_mr_params()
  MAPREDUCE_SPEC_PARAMS.update({"foo": "bar",
                                "base_path": "/foo",
                                "queue_name": QUEUE})
  HOOKS = __name__ + ".TestHooks"

  def setUp(self):
    super().setUp()
    self.mapper_spec = model.MapperSpec(
        handler_spec=self.HANLDER_SPEC,
        input_reader_spec=self.INPUT_READER_SPEC,
        params={"input_reader": {"entity_kind": self.ENTITY_KIND}},
        shard_count=self.SHARD_COUNT,
        output_writer_spec=self.OUTPUT_WRITER_SPEC)
    TestHooks.reset()

  def assertSuccess(self, mr_id, hooks_class_name=None):
    # Create spec from test setup data.
    mapreduce_spec = model.MapreduceSpec(
        name=self.NAME,
        mapreduce_id=mr_id,
        mapper_spec=self.mapper_spec.to_json(),
        params=self.MAPREDUCE_SPEC_PARAMS,
        hooks_class_name=hooks_class_name)

    # Verify state.
    state = model.MapreduceState.get_by_job_id(mr_id)
    self.assertEqual(mapreduce_spec, state.mapreduce_spec)
    self.assertTrue(state.active)
    self.assertEqual(0, state.active_shards)

    # Verify task.
    tasks = self.taskqueue.GetTasks(self.QUEUE)
    self.assertEqual(1, len(tasks))
    task = tasks[0]
    task_mr_id = test_support.decode_task_payload(task).get("mapreduce_id")
    self.assertEqual(mr_id, task_mr_id)
    # Check task headers.
    headers = dict(task["headers"])
    self.assertEqual(mr_id, headers[util._MR_ID_TASK_HEADER])
    self.assertTrue(headers["Host"], self.host)
    self.assertEqual("/foo/kickoffjob_callback/" + mapreduce_spec.mapreduce_id,
                     task["url"])

  def testSmoke(self):
    mr_id = handlers.StartJobHandler._start_map(
        self.NAME, self.mapper_spec,
        mapreduce_params=self.MAPREDUCE_SPEC_PARAMS,
        queue_name=self.QUEUE)

    self.assertSuccess(mr_id)

  def testStartWithOpenedTxn(self):
    @db.transactional(xg=True)
    def txn():
      # Four dummy entities to fill transaction.
      for _ in range(4):
        TestEntity().put()
      return handlers.StartJobHandler._start_map(
          self.NAME, self.mapper_spec,
          mapreduce_params=self.MAPREDUCE_SPEC_PARAMS,
          queue_name=self.QUEUE,
          in_xg_transaction=True)
    mr_id = txn()

    self.assertSuccess(mr_id)

  def testStartWithIndependentTxn(self):
    """Tests MR uses independent txn."""
    # Tests MR txn doesn't interfere with outer one.
    @db.transactional()
    def txn():
      # Put a dummy entity to fill the transaction.
      TestEntity().put()
      return handlers.StartJobHandler._start_map(
          self.NAME, self.mapper_spec,
          mapreduce_params=self.MAPREDUCE_SPEC_PARAMS,
          queue_name=self.QUEUE,
          in_xg_transaction=False)
    mr_id = txn()

    self.assertSuccess(mr_id)

  def testWithHooks(self):
    mr_id = handlers.StartJobHandler._start_map(
        self.NAME, self.mapper_spec,
        mapreduce_params=self.MAPREDUCE_SPEC_PARAMS,
        queue_name=self.QUEUE,
        hooks_class_name=self.HOOKS)

    self.assertSuccess(mr_id, self.HOOKS)
    self.assertEqual(1, len(TestHooks.enqueue_kickoff_task_calls))

  def testOtherApp(self):
    mr_id = handlers.StartJobHandler._start_map(
        self.NAME, self.mapper_spec,
        mapreduce_params=self.MAPREDUCE_SPEC_PARAMS,
        queue_name=self.QUEUE,
        _app="otherapp")

    self.assertSuccess(mr_id)
    state = model.MapreduceState.get_by_job_id(mr_id)
    self.assertEqual("otherapp", state.app_id)

  def testHandlerUnknown(self):
    """Tests when the handler function cannot be found."""
    self.mapper_spec.handler_spec = "does_not_exists"
    self.assertRaises(ImportError, handlers.StartJobHandler._start_map,
                      self.NAME, self.mapper_spec, self.MAPREDUCE_SPEC_PARAMS,
                      queue_name=self.QUEUE)

  def testInputReaderUnknown(self):
    """Tests when the input reader function cannot be found."""
    self.mapper_spec.input_reader_spec = "does_not_exists"
    self.assertRaises(ImportError, handlers.StartJobHandler._start_map,
                      self.NAME, self.mapper_spec, self.MAPREDUCE_SPEC_PARAMS,
                      queue_name=self.QUEUE)

  def testInvalidOutputWriter(self):
    """Tests setting output writer parameter."""
    self.mapper_spec.output_writer_spec = "does_not_exists"
    self.assertRaises(ImportError, handlers.StartJobHandler._start_map,
                      self.NAME, self.mapper_spec, self.MAPREDUCE_SPEC_PARAMS,
                      queue_name=self.QUEUE)


class KickOffJobHandlerTest(testutil.HandlerTestBase):
  """Test handlers.StartJobHandler."""

  NAME = "my_job"
  HANLDER_SPEC = MAPPER_HANDLER_SPEC
  ENTITY_KIND = __name__ + "." + TestEntity.__name__
  INPUT_READER_SPEC = __name__ + ".InputReader"
  OUTPUT_WRITER_SPEC = __name__ + ".TestOutputWriter"
  SHARD_COUNT = "9"
  QUEUE = "crazy-queue"
  MAPREDUCE_SPEC_PARAMS = map_job.JobConfig._get_default_mr_params()
  MAPREDUCE_SPEC_PARAMS.update({"foo": "bar",
                                "base_path": parameters.config.BASE_PATH,
                                "queue_name": QUEUE})
  HOOKS = __name__ + ".TestHooks"

  def setUp(self):
    super().setUp()
    TestHooks.reset()
    InputReader.reset()
    TestOutputWriter.reset()
    self._original_reschedule = handlers.ControllerCallbackHandler.reschedule

  def tearDown(self):
    super().tearDown()
    handlers.ControllerCallbackHandler.reschedule = self._original_reschedule

  def createDummyHandler(self):
    self.handler = handlers.KickOffJobHandler()
    self.mapreduce_id = "foo_id"

  def testInvalidMRState(self):
    self.createDummyHandler()
    # No mr_state exists.
    with self.app.test_request_context(
      query_string={"mapreduce_id": self.mapreduce_id},
      headers={"X-AppEngine-QueueName": self.QUEUE}):
      self.handler.post()

    self.assertEqual(0, len(self.taskqueue.GetTasks(self.QUEUE)))

    # mr_state is not active.
    state = model.MapreduceState.create_new(self.mapreduce_id)
    state.active = False
    state.put()

    with self.app.test_request_context(
      query_string={"mapreduce_id": self.mapreduce_id},
      headers={"X-AppEngine-QueueName": self.QUEUE}):
      self.handler.post()
    self.assertEqual(0, len(self.taskqueue.GetTasks(self.QUEUE)))

  def setUpValidState(self, hooks_class_name=None):
    self.mapper_spec = model.MapperSpec(
        handler_spec=self.HANLDER_SPEC,
        input_reader_spec=self.INPUT_READER_SPEC,
        params={"entity_kind": self.ENTITY_KIND},
        shard_count=self.SHARD_COUNT,
        output_writer_spec=self.OUTPUT_WRITER_SPEC)

    for _ in range(10):
      TestEntity().put()

    # Use StartJobHandler for setup.
    self.mr_id = handlers.StartJobHandler._start_map(
        self.NAME, self.mapper_spec,
        mapreduce_params=self.MAPREDUCE_SPEC_PARAMS,
        queue_name=self.QUEUE,
        hooks_class_name=hooks_class_name)

  def assertSuccess(self):
    # Verify states and that input reader split has been called.
    state = model.MapreduceState.get_by_job_id(self.mr_id)
    self.assertTrue(state.active)
    self.assertTrue(int(self.SHARD_COUNT), state.active_shards)
    shard_states = list(model.ShardState.find_all_by_mapreduce_state(state))
    self.assertEqual(int(self.SHARD_COUNT), len(shard_states))
    for ss in shard_states:
      self.assertTrue(ss.active)

    # Verify tasks.
    tasks = self.taskqueue.GetTasks(self.QUEUE)
    worker_tasks = 0
    controller_tasks = 0
    for task in tasks:
      self.assertEqual(self.QUEUE, task["queue_name"])
      # Check task headers.
      headers = dict(task["headers"])
      self.assertEqual(self.mr_id, headers[util._MR_ID_TASK_HEADER])
      self.assertEqual(self.host, headers["Host"])
      if task["url"].startswith("/mapreduce/worker_callback"):
        worker_tasks += 1
      if task["url"].startswith("/mapreduce/controller_callback"):
        controller_tasks += 1
    self.assertEqual(int(self.SHARD_COUNT), worker_tasks)
    self.assertEqual(1, controller_tasks)

  def testSmoke(self):
    self.setUpValidState()
    test_support.execute_all_tasks(self.taskqueue, self.QUEUE)

    self.assertSuccess()

    # Verify output writers has been created.
    self.assertEqual(
        ["init_job", "create-0", "create-1", "create-2", "create-3",
         "create-4", "create-5", "create-6", "create-7", "create-8"],
        TestOutputWriter.events)

  def testDropGracefully(self):
    self.setUpValidState()
    def always_fail(*args, **kwds):
      raise Exception("Raise an exception for test.")
    handlers.ControllerCallbackHandler.reschedule = always_fail

    test_support.execute_until_empty(self.taskqueue, self.QUEUE)

    # Final state is set correctly.
    state = model.MapreduceState.get_by_job_id(self.mr_id)
    self.assertFalse(state.active)
    self.assertEqual(model.MapreduceState.RESULT_FAILED, state.result_status)

    # Abort command is issued.
    self.assertTrue(model.MapreduceControl.get_key_by_job_id(self.mr_id))

    # If a shard was started, then it has been aborted.
    shard_states = list(model.ShardState.find_all_by_mapreduce_state(state))
    for ss in shard_states:
      self.assertFalse(ss.active)
      self.assertEqual(model.ShardState.RESULT_ABORTED, ss.result_status)

    # Cleanup was called.
    trash = list(model._HugeTaskPayload.all().ancestor(state).run())
    self.assertFalse(trash)

  def testWithHooks(self):
    self.setUpValidState(self.HOOKS)
    test_support.execute_all_tasks(self.taskqueue, self.QUEUE)

    self.assertSuccess()
    self.assertEqual(1, len(TestHooks.enqueue_controller_task_calls))
    self.assertEqual(int(self.SHARD_COUNT),
                     len(TestHooks.enqueue_worker_task_calls))

  def testNoInput(self):
    self.INPUT_READER_SPEC = __name__ + ".EmptyInputReader"
    self.setUpValidState()
    test_support.execute_all_tasks(self.taskqueue, self.QUEUE)
    state = model.MapreduceState.get_by_job_id(self.mr_id)
    self.assertFalse(state.active)
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS, state.result_status)

  def testGetInputReaders(self):
    self.createDummyHandler()
    self.setUpValidState()
    state = model.MapreduceState.get_by_job_id(self.mr_id)
    readers, serialized_readers_entity = (
        self.handler._get_input_readers(state))

    self.assertEqual(int(self.SHARD_COUNT), len(readers))
    self.assertEqual(int(self.SHARD_COUNT), state.active_shards)
    self.assertEqual(int(self.SHARD_COUNT),
                     state.mapreduce_spec.mapper.shard_count)
    self.assertTrue(self.handler._save_states(state, serialized_readers_entity))

    # Call again to test idempotency.
    new_readers, new_serialized_readers_entity = (
        self.handler._get_input_readers(state))
    # Serialized new readers should be the same as serialized old ones.
    self.assertEqual(serialized_readers_entity.payload,
                     new_serialized_readers_entity.payload)

  def testSaveState(self):
    self.createDummyHandler()
    self.setUpValidState()
    state = model.MapreduceState.get_by_job_id(self.mr_id)
    _, serialized_readers_entity = (
        self.handler._get_input_readers(state))
    self.assertTrue(self.handler._save_states(state, serialized_readers_entity))
    # Call again to test idempotency.
    self.assertEqual(
        None, self.handler._save_states(state, serialized_readers_entity))

  def testScheduleShards(self):
    self.createDummyHandler()
    self.setUpValidState()
    self.taskqueue.FlushQueue(self.QUEUE)
    state = model.MapreduceState.get_by_job_id(self.mr_id)
    readers, _ = (
        self.handler._get_input_readers(state))
    self.handler._schedule_shards(state.mapreduce_spec, readers, self.QUEUE,
                                  "/foo", state)

    shard_states = list(model.ShardState.find_all_by_mapreduce_state(state))
    self.assertEqual(int(self.SHARD_COUNT), len(shard_states))
    for ss in shard_states:
      self.assertTrue(ss.active)

    # Verify output writers has been created.
    self.assertEqual(
        ["create-0", "create-1", "create-2", "create-3",
         "create-4", "create-5", "create-6", "create-7", "create-8"],
        TestOutputWriter.events)

    # Verify tasks.
    tasks = self.taskqueue.GetTasks(self.QUEUE)
    worker_tasks = 0
    for task, ss in zip(tasks, shard_states):
      self.assertEqual("/foo/worker_callback/" + ss.shard_id, task["url"])
      worker_tasks += 1
    self.assertEqual(int(self.SHARD_COUNT), worker_tasks)

    # Call again to test idempotency.
    self.handler._schedule_shards(state.mapreduce_spec, readers, self.QUEUE,
                                  "/foo", state)
    self.assertEqual(int(self.SHARD_COUNT),
                     len(self.taskqueue.GetTasks(self.QUEUE)))
    new_shard_states = model.ShardState.find_all_by_mapreduce_state(state)
    for o, n in zip(shard_states, new_shard_states):
      self.assertEqual(o, n)


class MapperWorkerCallbackHandlerLeaseTest(testutil.HandlerTestBase):
  """Test lease related logics of handlers.MapperWorkerCallbackHandler.

  These tests creates a WorkerHandler for the same shard.
  WorkerHandler gets a payload that's (in)consistent with datastore's
  ShardState in some way.
  """

  # This shard's number.
  SHARD_NUMBER = 1
  # Current slice id in datastore.
  CURRENT_SLICE_ID = 3
  CURRENT_REQUEST_ID = "20150131a"
  # Request id from the previous slice execution.
  PREVIOUS_REQUEST_ID = "19991231a"

  def setUp(self):
    super().setUp()
    os.environ["REQUEST_LOG_ID"] = self.CURRENT_REQUEST_ID

    self.mr_spec = None
    self._init_job()

    self.shard_id = None
    self.shard_state = None
    self._init_shard()

    self.mr_state = None
    self._init_mr_state()

    self._original_duration = parameters.config._SLICE_DURATION_SEC
    # Make sure handler can process at most one entity and thus
    # shard will still be in active state after one call.
    parameters.config._SLICE_DURATION_SEC = 0

  def tearDown(self):
    parameters.config._SLICE_DURATION_SEC = self._original_duration
    super().tearDown()

  def _init_job(self, handler_spec=MAPPER_HANDLER_SPEC):
    """Init job specs."""
    mapper_spec = model.MapperSpec(
        handler_spec=handler_spec,
        input_reader_spec=__name__ + "." + InputReader.__name__,
        params={"input_reader": {"entity_kind": ENTITY_KIND}},
        shard_count=self.SHARD_NUMBER)
    self.mr_spec = model.MapreduceSpec(
        name="mapreduce_name",
        mapreduce_id="mapreduce_id",
        mapper_spec=mapper_spec.to_json(),
        params=map_job.JobConfig._get_default_mr_params())

  def _init_shard(self):
    """Init shard state."""
    self.shard_state = model.ShardState.create_new(
        self.mr_spec.mapreduce_id,
        shard_number=self.SHARD_NUMBER)
    self.shard_state.slice_id = self.CURRENT_SLICE_ID
    self.shard_state.put()
    self.shard_id = self.shard_state.shard_id

  def _init_mr_state(self):
    self.mr_state = model.MapreduceState.create_new(mapreduce_id="mapreduce_id")
    self.mr_state.mapreduce_spec = self.mr_spec
    self.mr_state.put()

  def _create_handler(self, slice_id=CURRENT_SLICE_ID):
    """Create a handler instance with payload for a particular slice."""
    # Reset map handler.
    TestHandler.reset()

    # Create input reader and test entity.
    InputReader.reset()
    TestEntity().put()
    TestEntity().put()
    reader_iter = db_iters.RangeIteratorFactory.create_key_ranges_iterator(
        key_ranges.KeyRangesFactory.create_from_list([key_range.KeyRange()]),
        model.QuerySpec("ENTITY_KIND", model_class_path=ENTITY_KIND),
        db_iters.KeyRangeModelIterator)

    # Create worker handler.
    handler = handlers.MapperWorkerCallbackHandler()

    # Create transient shard state.
    tstate = model.TransientShardState(
        base_path="base_path",
        mapreduce_spec=self.mr_spec,
        shard_id=self.shard_state.shard_id,
        slice_id=slice_id,
        input_reader=InputReader(reader_iter),
        initial_input_reader=InputReader(reader_iter))

    return handler, tstate

  def assertNoEffect(self, no_shard_state=False):
    """Assert shard state and taskqueue didn't change."""
    stub = apiproxy_stub_map.apiproxy.GetStub("taskqueue")
    self.assertEqual(0, len(stub.GetTasks("default")))

    shard_state = model.ShardState.get_by_shard_id(self.shard_state.shard_id)

    if not no_shard_state:
      assert shard_state

    if shard_state:
      # sync auto_now field
      shard_state.update_time = self.shard_state.update_time
      self.assertEqual(str(self.shard_state), str(shard_state))

  def testStateNotFound(self):
    handler, tstate = self._create_handler()
    self.shard_state.delete()

    with self.app.test_request_context(
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mr_spec.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=tstate.to_dict()
    ):
      handler.dispatch_request()

    self.assertNoEffect(no_shard_state=True)
    self.assertEqual(None, model.ShardState.get_by_shard_id(self.shard_id))

  def testStateNotActive(self):
    handler, tstate = self._create_handler()
    self.shard_state.active = False
    self.shard_state.put()
    with self.app.test_request_context(
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mr_spec.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=tstate.to_dict()
    ):
      handler.dispatch_request()
    self.assertNoEffect()

  def testOldTask(self):
    handler, tstate = self._create_handler(slice_id=self.CURRENT_SLICE_ID - 1)
    with self.app.test_request_context(
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mr_spec.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=tstate.to_dict()
    ):
      handler.dispatch_request()
    self.assertNoEffect()

  def testFutureTask(self):
    handler, tstate = self._create_handler(slice_id=self.CURRENT_SLICE_ID + 1)
    with self.app.test_request_context(
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mr_spec.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=tstate.to_dict()
    ), self.assertRaises(werkzeug.exceptions.ServiceUnavailable):
      handler.dispatch_request()

  def testLeaseHasNotEnd(self):
    self.shard_state.slice_start_time = datetime.datetime.now()
    self.shard_state.put()
    handler, tstate = self._create_handler()

    with mock.patch("datetime.datetime", autospec=True) as dt:
      # One millisecond after.
      dt.now.return_value = (self.shard_state.slice_start_time +
                             datetime.timedelta(milliseconds=1))
      self.assertEqual(
          math.ceil(parameters._LEASE_DURATION_SEC),
          handler._wait_time(self.shard_state,
                             parameters._LEASE_DURATION_SEC))
      with self.app.test_request_context(
        method="POST",
        headers={
          model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
          "X-AppEngine-QueueName": "default",
          "X-Appengine-TaskName": "task_name",
          util._MR_ID_TASK_HEADER: self.mr_spec.mapreduce_id,
          util._MR_SHARD_ID_TASK_HEADER: self.shard_id
        }, 
        data=tstate.to_dict()
      ), self.assertRaises(werkzeug.exceptions.ServiceUnavailable):
        handler.dispatch_request()

  def testRequestHasNotEnd(self):
    # Previous request's lease has timed out but the request has not.
    now = datetime.datetime.now()
    old = (now -
           datetime.timedelta(seconds=parameters._LEASE_DURATION_SEC + 1))
    self.shard_state.slice_start_time = old
    self.shard_state.slice_request_id = self.PREVIOUS_REQUEST_ID
    self.shard_state.put()
    handler, tstate = self._create_handler()

    with self.app.test_request_context(
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mr_spec.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=tstate.to_dict()
    ), self.assertRaises(werkzeug.exceptions.ServiceUnavailable):
      handler.dispatch_request()

  def testRequestHasTimedOut(self):
    slice_start_time = datetime.datetime(2000, 1, 1)
    self.shard_state.slice_start_time = slice_start_time
    self.shard_state.slice_request_id = self.PREVIOUS_REQUEST_ID
    self.shard_state.put()
    handler, tstate = self._create_handler()

    # acquire lease should succeed.
    handler._try_acquire_lease(self.shard_state, tstate)

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertTrue(shard_state.active)
    self.assertEqual(self.CURRENT_SLICE_ID, shard_state.slice_id)
    self.assertEqual(self.CURRENT_REQUEST_ID, shard_state.slice_request_id.decode())
    self.assertTrue(shard_state.slice_start_time > slice_start_time)

  def testContentionWhenAcquireLease(self):
    # Shard has moved on AFTER we got shard state.
    self.shard_state.slice_id += 1
    self.shard_state.put()

    # Revert in memory shard state.
    self.shard_state.slice_id -= 1
    handler, tstate = self._create_handler()
    self.assertEqual(
        handler._TASK_DIRECTIVE.RETRY_TASK,
        # Use old shard state.
        handler._try_acquire_lease(self.shard_state, tstate))

  def testAcquireLeaseSuccess(self):
    # lease acquired a long time ago.
    slice_start_time = datetime.datetime(2000, 1, 1)
    self.shard_state.slice_start_time = slice_start_time
    self.shard_state.slice_request_id = self.PREVIOUS_REQUEST_ID
    self.shard_state.put()
    handler, tstate = self._create_handler()

    handler._try_acquire_lease(self.shard_state, tstate)

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertTrue(shard_state.active)
    self.assertEqual(self.CURRENT_SLICE_ID, shard_state.slice_id)
    self.assertEqual(self.CURRENT_REQUEST_ID, shard_state.slice_request_id.decode())
    self.assertTrue(shard_state.slice_start_time > slice_start_time)
    self.assertEqual(shard_state, self.shard_state)

  def testLeaseFreedOnSuccess(self):
    self.shard_state.slice_start_time = datetime.datetime(2000, 1, 1)
    self.shard_state.slice_request_id = self.PREVIOUS_REQUEST_ID
    self.shard_state.put()
    handler, tstate = self._create_handler()

    with self.app.test_request_context(
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mr_spec.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=tstate.to_dict()
    ):
      handler.dispatch_request()

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertTrue(shard_state.active)
    # Slice moved on.
    self.assertEqual(self.CURRENT_SLICE_ID + 1, shard_state.slice_id)
    # Lease is freed.
    self.assertFalse(shard_state.slice_start_time)
    self.assertFalse(shard_state.slice_request_id)
    stub = apiproxy_stub_map.apiproxy.GetStub("taskqueue")
    self.assertEqual(1, len(stub.GetTasks("default")))

  def testLeaseFreedOnSliceRetry(self):
    # Reinitialize with faulty map function.
    self._init_job(__name__ + "." + fake_handler_raise_exception.__name__)
    self._init_shard()
    handler, tstate = self._create_handler()

    with self.app.test_request_context(
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mr_spec.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=tstate.to_dict()
    ), self.assertRaises(werkzeug.exceptions.ServiceUnavailable):
      handler.dispatch_request()

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertTrue(shard_state.active)
    # Slice stays the same.
    self.assertEqual(self.CURRENT_SLICE_ID, shard_state.slice_id)
    # Lease is freed.
    self.assertFalse(shard_state.slice_start_time)
    self.assertFalse(shard_state.slice_request_id)
    # Slice retry is increased.
    self.assertEqual(self.shard_state.slice_retries + 1,
                     shard_state.slice_retries)

  def testLeaseFreedOnTaskqueueUnavailable(self):
    handler, tstate = self._create_handler()
    with mock.patch("mapreduce"
                    ".handlers.MapperWorkerCallbackHandler._add_task") as add:
      add.side_effect = taskqueue.Error
      with self.app.test_request_context(
        method="POST",
        headers={
          model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
          "X-AppEngine-QueueName": "default",
          "X-Appengine-TaskName": "task_name",
          util._MR_ID_TASK_HEADER: self.mr_spec.mapreduce_id,
          util._MR_SHARD_ID_TASK_HEADER: self.shard_id
        }, 
        data=tstate.to_dict()
      ), self.assertRaises(taskqueue.Error):
        handler.dispatch_request()

    # No new task in taskqueue.
    stub = apiproxy_stub_map.apiproxy.GetStub("taskqueue")
    self.assertEqual(0, len(stub.GetTasks("default")))

    shard_state = model.ShardState.get_by_shard_id(self.shard_state.shard_id)
    self.assertTrue(shard_state.acquired_once)
    # Besides these fields, all other fields should be the same.
    shard_state.acquired_once = self.shard_state.acquired_once
    shard_state.update_time = self.shard_state.update_time
    self.assertEqual(str(self.shard_state), str(shard_state))


class MapperWorkerCallbackHandlerTest(MapreduceHandlerTestBase):
  """Test handlers.MapperWorkerCallbackHandler."""

  def setUp(self):
    """Sets up the test harness."""
    MapreduceHandlerTestBase.setUp(self)
    self.original_task_add = taskqueue.Task.add
    self.original_slice_duration = parameters.config._SLICE_DURATION_SEC
    self.original_task_max_data_processing_attempts = (
        parameters.config.TASK_MAX_DATA_PROCESSING_ATTEMPTS)
    self.original_supports_slice_recovery = (
        TestOutputWriter._supports_slice_recovery)
    self.init()

  def tearDown(self):
    handlers._TEST_INJECTED_FAULTS.clear()
    taskqueue.Task.add = self.original_task_add
    parameters.config._SLICE_DURATION_SEC = self.original_slice_duration
    parameters.config.TASK_MAX_DATA_PROCESSING_ATTEMPTS = (
        self.original_task_max_data_processing_attempts)
    TestOutputWriter._supports_slice_recovery = (
        self.original_supports_slice_recovery)
    MapreduceHandlerTestBase.tearDown(self)

  def init(self,
           mapper_handler_spec=MAPPER_HANDLER_SPEC,
           mapper_parameters=None,
           hooks_class_name=None,
           output_writer_spec=None,
           shard_count=8,
           mr_params=None,
           shard_retries=0):
    """Init everything needed for testing worker callbacks.

    Args:
      mapper_handler_spec: handler specification to use in test.
      mapper_params: mapper specification to use in test.
      hooks_class_name: fully qualified name of the hooks class to use in test.
    """
    InputReader.reset()

    self.mapreduce_id = "mapreduce0"
    self.mapreduce_spec = self.create_mapreduce_spec(
        self.mapreduce_id,
        shard_count,
        mapper_handler_spec=mapper_handler_spec,
        hooks_class_name=hooks_class_name,
        output_writer_spec=output_writer_spec,
        mapper_parameters=mapper_parameters)
    if mr_params:
      self.mapreduce_spec.params.update(mr_params)

    self.shard_number = 1
    self.slice_id = 0
    self.shard_state = self.create_and_store_shard_state(
        self.mapreduce_id, self.shard_number)
    self.shard_state.retries = shard_retries
    self.shard_state.slice_id = self.slice_id
    self.shard_state.put()
    self.shard_id = self.shard_state.shard_id

    output_writer = None
    if self.mapreduce_spec.mapper.output_writer_class():
      output_writer_cls = self.mapreduce_spec.mapper.output_writer_class()
      output_writer = output_writer_cls.create(self.mapreduce_spec,
                                               self.shard_number,
                                               shard_retries + 1)

    reader_iter = db_iters.RangeIteratorFactory.create_key_ranges_iterator(
        key_ranges.KeyRangesFactory.create_from_list([key_range.KeyRange()]),
        model.QuerySpec("ENTITY_KIND", model_class_path=ENTITY_KIND),
        db_iters.KeyRangeModelIterator)
    self.transient_state = model.TransientShardState(
        "/mapreduce",
        self.mapreduce_spec,
        self.shard_id,
        self.slice_id,
        InputReader(reader_iter),
        InputReader(reader_iter),
        output_writer=output_writer,
        retries=shard_retries)

    self.handler = handlers.MapperWorkerCallbackHandler()
    self.handler._time = MockTime.time

  def _handle_request(self, expect_finalize=True):
    """Handles request and optionally finalizes the output stream."""
    self.assertEqual(0, len(self.taskqueue.GetTasks("default")))

    with self.app.test_request_context(
      "/mapreduce/worker_callback/" + self.shard_id,
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=self.transient_state.to_dict()
    ):
      self.handler.dispatch_request()

    if not expect_finalize:
      self.assertEqual(0, len(self.taskqueue.GetTasks("default")))
      return

    # We processed all the input but we still need to finalize (on a separate
    # slice).

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertTrue(shard_state.input_finished)
    self.assertTrue(shard_state.active)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    self.verify_shard_task(
        tasks[0], self.shard_id, slice_id=1, verify_spec=False)
    self.taskqueue.FlushQueue("default")

    with self.app.test_request_context(
      "/mapreduce/worker_callback/" + self.shard_id,
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id,
      }, 
      data={**self.transient_state.to_dict(), "slice_id": 1}
    ):
      self.handler.dispatch_request()

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertFalse(shard_state.active)

  def testDecodingPayloadFailed(self):
    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertTrue(shard_state.active)

    with self.app.test_request_context(
      "/mapreduce/worker_callback/" + self.shard_id,
      method="POST",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=self.transient_state.to_dict()
    ):
      self.handler.dispatch_request()

    self.assertEqual(0, len(self.taskqueue.GetTasks("default")))

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertFalse(shard_state.active)
    self.assertEqual(model.ShardState.RESULT_FAILED, shard_state.result_status)

  def testSmoke(self):
    """Test main execution path of entity scanning.

    No processing rate limit.
    """
    e1 = TestEntity()
    e1.put()

    e2 = TestEntity()
    e2.put()

    self._handle_request()

    self.assertEqual([str(e1.key()), str(e2.key())],
                      TestHandler.processed_keys)

    # we should have finished
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False, processed=2, input_finished=True,
        result_status=model.ShardState.RESULT_SUCCESS)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(0, len(tasks))

  def testMaintainLCNoOp(self):
    shard_ctx = mock.MagicMock()
    slice_ctx = mock.MagicMock()
    # Do nothing if the object doesn't implement shard_life_cycle interface.
    self.handler._maintain_LC(object(), 1, shard_ctx=shard_ctx,
                              slice_ctx=slice_ctx)

  def testMaintainLCBeginShard(self):
    obj = mock.Mock(spec=shard_life_cycle._ShardLifeCycle)
    shard_ctx = mock.MagicMock()
    slice_ctx = mock.MagicMock()
    self.handler._maintain_LC(obj, 0, shard_ctx=shard_ctx,
                              slice_ctx=slice_ctx)
    self.assertEqual(
        obj.mock_calls,
        [mock.call.begin_shard(shard_ctx),
         mock.call.begin_slice(slice_ctx)])

  def testMaintainLCEndShard(self):
    obj = mock.Mock(spec=shard_life_cycle._ShardLifeCycle)
    shard_ctx = mock.MagicMock()
    slice_ctx = mock.MagicMock()
    self.handler._maintain_LC(obj, 0, begin_slice=False, last_slice=True,
                              shard_ctx=shard_ctx, slice_ctx=slice_ctx)
    self.assertEqual(
        obj.mock_calls,
        [mock.call.end_slice(slice_ctx),
         mock.call.end_shard(shard_ctx)])

  def testMaintainLCBeginSlice(self):
    obj = mock.Mock(spec=shard_life_cycle._ShardLifeCycle)
    slice_ctx = mock.MagicMock()
    shard_ctx = mock.MagicMock()
    self.handler._maintain_LC(obj, 1, slice_ctx=slice_ctx,
                              shard_ctx=shard_ctx)
    self.assertEqual(
        obj.mock_calls,
        [mock.call.begin_slice(slice_ctx)])

  def testMaintainLCEndSlice(self):
    obj = mock.Mock(spec=shard_life_cycle._ShardLifeCycle)
    slice_ctx = mock.MagicMock()
    shard_ctx = mock.MagicMock()
    self.handler._maintain_LC(obj, 1, begin_slice=False,
                              shard_ctx=shard_ctx, slice_ctx=slice_ctx)
    self.assertEqual(
        obj.mock_calls,
        [mock.call.end_slice(slice_ctx)])

  def testLCBeginSliceCallOrdering(self):
    parent = mock.MagicMock()
    parent.handler = mock.Mock(spec=shard_life_cycle._ShardLifeCycle)
    parent.input_reader = mock.Mock(spec=shard_life_cycle._ShardLifeCycle)
    parent.output_writer = mock.Mock(spec=shard_life_cycle._ShardLifeCycle)

    TState = collections.namedtuple(
        "TState", ["handler", "input_reader", "output_writer"])
    tstate = TState(parent.handler, parent.input_reader, parent.output_writer)

    self.handler._lc_start_slice(tstate, 42)
    parent.assert_has_calls([mock.call.output_writer.begin_slice(None),
                             mock.call.input_reader.begin_slice(None),
                             mock.call.handler.begin_slice(None)])

  def testLCEndSliceCallOrdering(self):
    parent = mock.MagicMock()
    parent.handler = mock.Mock(spec=shard_life_cycle._ShardLifeCycle)
    parent.input_reader = mock.Mock(spec=shard_life_cycle._ShardLifeCycle)
    parent.output_writer = mock.Mock(spec=shard_life_cycle._ShardLifeCycle)

    TState = collections.namedtuple(
        "TState", ["handler", "input_reader", "output_writer"])
    tstate = TState(parent.handler, parent.input_reader, parent.output_writer)

    self.handler._lc_end_slice(tstate, 42)
    parent.assert_has_calls([mock.call.handler.end_slice(None),
                             mock.call.input_reader.end_slice(None),
                             mock.call.output_writer.end_slice(None)])

  def testCompletedState(self):
    self.shard_state.input_finished = True
    self.shard_state.active = False
    self.shard_state.put()

    e1 = TestEntity()
    e1.put()

    self._handle_request(expect_finalize=False)

    # completed state => no data processed
    self.assertEqual([], TestHandler.processed_keys)

    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False,
        input_finished=True)

    self.assertEqual(0, len(self.taskqueue.GetTasks("default")))

  def testAllInputProcessedStopsProcessing(self):
    self.shard_state.input_finished = True
    self.shard_state.put()

    e1 = TestEntity()
    e1.put()

    with self.app.test_request_context(
      "/mapreduce/worker_callback/" + self.shard_id,
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=self.transient_state.to_dict()
    ):
      self.handler.dispatch_request()

    # completed state => no data processed
    self.assertEqual([], TestHandler.processed_keys)

    # but shard is done now
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False,
        result_status=model.ShardState.RESULT_SUCCESS,
        input_finished=True)


  def testShardStateCollision(self):
    handlers._TEST_INJECTED_FAULTS.add("worker_active_state_collision")

    e1 = TestEntity()
    e1.put()

    self._handle_request(expect_finalize=False)

    # Data will still be processed
    self.assertEqual([str(e1.key())], TestHandler.processed_keys)
    # Shard state should not be overriden, i.e. left active.
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id), active=True)
    self.assertEqual(0, len(self.taskqueue.GetTasks("default")))

  def testNoShardState(self):
    """Correct handling of missing shard state."""
    self.shard_state.delete()
    e1 = TestEntity()
    e1.put()

    self._handle_request(expect_finalize=False)

    # no state => no data processed
    self.assertEqual([], TestHandler.processed_keys)
    self.assertEqual(0, len(self.taskqueue.GetTasks("default")))

  def testNoData(self):
    """Test no data to scan case."""
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id), active=True)

    self._handle_request()

    self.assertEqual([], TestHandler.processed_keys)

    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False, input_finished=True,
        result_status=model.ShardState.RESULT_SUCCESS)

  def testUserAbort(self):
    """Tests a user-initiated abort of the shard."""
    # Be sure to have an output writer for the abort step so we can confirm
    # that the finalize() method is never called.
    self.init(__name__ + ".fake_handler_yield_keys",
              output_writer_spec=__name__ + ".UnfinalizableTestOutputWriter")

    model.MapreduceControl.abort(self.mapreduce_id, force_writes=True)
    self._handle_request(expect_finalize=False)
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False,
        result_status=model.ShardState.RESULT_ABORTED)

  def testLifeCycle(self):
    """Tests life cycle methods are called."""
    self.init(__name__ + ".fake_handler_yield_keys",
              output_writer_spec=__name__ + ".ShardLifeCycleOutputWriter")

    e1 = TestEntity()
    e1.put()

    self._handle_request()
    expected_events = [
        "create-1",
        "begin_shard-mapreduce0-1",
        "begin_slice-0",
        "write-agx0ZXN0YmVkLXRlc3RyEAsSClRlc3RFbnRpdHkYAQw",
        "end_slice-0",
        "begin_slice-1",
        "end_slice-1",
        "end_shard-mapreduce0-1",
        "finalize-1"
    ]
    self.assertEqual(expected_events, ShardLifeCycleOutputWriter.events)

  def testLongProcessingShouldStartAnotherSlice(self):
    """Long scan.

    If scanning takes too long, it should be paused, and new continuation task
    should be spawned.
    """
    e1 = TestEntity()
    e1.put()

    e2 = TestEntity()
    e2.put()

    TestHandler.delay = parameters.config._SLICE_DURATION_SEC + 10

    with self.app.test_request_context(
      "/mapreduce/worker_callback/" + self.shard_id,
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=self.transient_state.to_dict()
    ):
      self.handler.dispatch_request()

    # only first entity should be processed
    self.assertEqual([str(e1.key())], TestHandler.processed_keys)

    # slice should be still active
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        processed=1)

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, self.slice_id + 1)
    self.assertEqual(tasks[0]["eta_delta"], "0:00:00 ago")

  @mock.patch.object(taskqueue, 'Task')
  def testLimitingRate(self, task_mock):
    """Test not enough quota to process everything in this slice."""
    e1 = TestEntity()
    e1.put()

    e2 = TestEntity()
    e2.put()

    e3 = TestEntity()
    e3.put()

    # Everytime the handler is called, it increases time by this amount.
    TestHandler.delay = parameters.config._SLICE_DURATION_SEC/2 - 1
    # handler should be called twice.
    self.init(mapper_parameters={
        "processing_rate": 10.0/parameters.config._SLICE_DURATION_SEC},
              shard_count=5)

    with self.app.test_request_context(
      "/mapreduce/worker_callback/" + self.shard_id,
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=self.transient_state.to_dict()
    ):
      self.handler.dispatch_request()

    self.assertEqual(2, len(TestHandler.processed_keys))
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True, processed=2,
        result_status=None)

    task_mock.assert_called_once()
    self.assertEqual(task_mock.call_args[1]["countdown"], 2)
    task_mock.return_value.add.assert_called_once()


  def testLongProcessDataWithAllowCheckpoint(self):
    """Tests that process_datum works with input_readers.ALLOW_CHECKPOINT."""
    self.handler._start_time = 0
    self.assertFalse(self.handler._process_datum(input_readers.ALLOW_CHECKPOINT,
                                                 None,
                                                 None,
                                                 None))

  def testScheduleSlice(self):
    """Test _schedule_slice method."""
    self.handler._schedule_slice(
        self.shard_state,
        model.TransientShardState(
            "/mapreduce", self.mapreduce_spec,
            self.shard_id, 123, mock.Mock(), mock.Mock()))

    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    # Verify task headers.
    headers = dict(tasks[0]["headers"])
    self.assertEqual(self.mapreduce_id, headers[util._MR_ID_TASK_HEADER])
    self.assertEqual(self.shard_id, headers[util._MR_SHARD_ID_TASK_HEADER])
    self.assertEqual(self.host, headers["Host"])

    self.verify_shard_task(tasks[0], self.shard_id, 123)

  def testScheduleSlice_Eta(self):
    """Test _schedule_slice method."""
    eta = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    self.handler._schedule_slice(
        self.shard_state,
        model.TransientShardState(
            "/mapreduce", self.mapreduce_spec,
            self.shard_id, 123, mock.Mock(), mock.Mock()),
        eta=eta)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, 123, eta=eta)

  def testScheduleSlice_Countdown(self):
    """Test _schedule_slice method."""
    countdown = 60 * 60
    self.handler._schedule_slice(
        self.shard_state,
        model.TransientShardState(
            "/mapreduce", self.mapreduce_spec,
            self.shard_id, 123, mock.Mock(), mock.Mock()),
        countdown=countdown)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, 123, countdown=countdown)

  def testScheduleSlice_QueuePreserved(self):
    """Tests that _schedule_slice will enqueue tasks on the calling queue."""
    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = "crazy-queue"
    try:
      self.handler._schedule_slice(
          self.shard_state,
          model.TransientShardState(
              "/mapreduce", self.mapreduce_spec,
              self.shard_id, 123, mock.Mock(), mock.Mock()))

      tasks = self.taskqueue.GetTasks("crazy-queue")
      self.assertEqual(1, len(tasks))
      self.verify_shard_task(tasks[0], self.shard_id, 123)
    finally:
      del os.environ["HTTP_X_APPENGINE_QUEUENAME"]

  def testScheduleSlice_TombstoneErrors(self):
    """Tests when the scheduled slice already exists."""
    self.handler._schedule_slice(self.shard_state, self.transient_state)

    # This catches the exception.
    self.handler._schedule_slice(self.shard_state, self.transient_state)

    # The task won't re-enqueue because it has the same name.
    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))

  def testScheduleSlice_Hooks(self):
    """Test _schedule_slice method with a hooks class installed."""
    hooks_class_name = __name__ + '.' + TestHooks.__name__
    self.init(hooks_class_name=hooks_class_name)

    self.handler._schedule_slice(self.shard_state, self.transient_state)

    self.assertEqual(1, len(self.taskqueue.GetTasks("default")))
    self.assertEqual(1, len(TestHooks.enqueue_worker_task_calls))
    task, queue_name = TestHooks.enqueue_worker_task_calls[0]
    self.assertEqual("/mapreduce/worker_callback/" + self.shard_state.shard_id,
                      task.url)
    self.assertEqual("default", queue_name)

  def testScheduleSlice_RaisingHooks(self):
    """Test _schedule_slice method with an empty hooks class installed.

    The installed hooks class will raise NotImplementedError in response to
    all method calls.
    """
    hooks_class_name = hooks.__name__ + '.' + hooks.Hooks.__name__
    self.init(hooks_class_name=hooks_class_name)

    self.handler._schedule_slice(
        self.shard_state,
        model.TransientShardState(
            "/mapreduce", self.mapreduce_spec,
            self.shard_id, 123, mock.Mock(), mock.Mock()))

    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, 123,
                           hooks_class_name=hooks_class_name)

  def testDatastoreExceptionInHandler(self):
    """Test when a handler can't save state to datastore."""
    self.init(__name__ + ".fake_handler_yield_keys")
    TestEntity().put()

    with mock.patch.object(datastore, 'PutAsync', side_effect=datastore_errors.Timeout()):
        # Tests that handler doesn't abort task for datastore errors.
        # Unfortunately they still increase TaskExecutionCount.
        for _ in range(parameters.config.TASK_MAX_DATA_PROCESSING_ATTEMPTS):
            with self.app.test_request_context(
                "/mapreduce/worker_callback/" + self.shard_id,
                method="POST",
                headers={
                    model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
                    "X-AppEngine-QueueName": "default",
                    "X-Appengine-TaskName": "task_name",
                    util._MR_ID_TASK_HEADER: self.mapreduce_id,
                    util._MR_SHARD_ID_TASK_HEADER: self.shard_id
                }, 
                data=self.transient_state.to_dict()
            ):
                self.assertRaises(datastore_errors.Timeout, self.handler.dispatch_request)
        self.verify_shard_state(
            model.ShardState.get_by_shard_id(self.shard_id),
            active=True,
            processed=0)
    
    self._handle_request()

    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False, input_finished=True,
        result_status=model.ShardState.RESULT_SUCCESS,
        processed=1)

  def testTaskqueueExceptionInHandler(self):
    """Test when a handler can't reach taskqueue."""
    self.init(__name__ + ".fake_handler_yield_keys")
    # Force enqueue another task.
    parameters.config._SLICE_DURATION_SEC = 0
    TestEntity().put()
    taskqueue.Task.add = mock.MagicMock(side_effect=taskqueue.TransientError)

    # Tests that handler doesn't abort task for taskqueue errors.
    # Unfornately they still increase TaskExecutionCount.
    for _ in range(parameters.config.TASK_MAX_DATA_PROCESSING_ATTEMPTS):
      with self.app.test_request_context(
        "/mapreduce/worker_callback/" + self.shard_id,
        method="POST",
        headers={
          model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
          "X-AppEngine-QueueName": "default",
          "X-Appengine-TaskName": "task_name",
          util._MR_ID_TASK_HEADER: self.mapreduce_id,
          util._MR_SHARD_ID_TASK_HEADER: self.shard_id
        }, 
        data=self.transient_state.to_dict()
      ):
        self.assertRaises(taskqueue.TransientError, self.handler.dispatch_request)
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        processed=0)

    taskqueue.Task.add = self.original_task_add

    with self.app.test_request_context(
      "/mapreduce/worker_callback/" + self.shard_id,
      method="POST",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=self.transient_state.to_dict()
    ):
      self.handler.post()

    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        result_status=None,
        processed=1)

  def testSliceRecoveryNotCalledWithNoOutputWriter(self):
    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    # This slice had acquired shard lock in previous attempt.
    shard_state.acquired_once = True
    shard_state.put()

    self._handle_request()
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False, input_finished=True,
        result_status=model.ShardState.RESULT_SUCCESS,
        processed=0)

  def testSliceRecoveryNotCalledWithOutputWriter(self):
    """Test when output writer doesn't support slice recovery."""
    self.init(output_writer_spec=__name__ + ".TestOutputWriter")
    TestOutputWriter._supports_slice_recovery = lambda self, spec: False

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    # This slice had acquired shard lock in previous attempt.
    shard_state.acquired_once = True
    shard_state.put()

    self._handle_request()
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False, input_finished=True,
        result_status=model.ShardState.RESULT_SUCCESS)
    self.assertFalse("recover" in TestOutputWriter.events)

  def testSliceRecoveryNotCalledWithOutputWriter2(self):
    """Test when the slice isn't a retry."""
    self.init(output_writer_spec=__name__ + ".TestOutputWriter")

    self._handle_request()
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False, input_finished=True,
        result_status=model.ShardState.RESULT_SUCCESS)
    self.assertFalse("recover" in TestOutputWriter.events)

  def testSliceRecoveryCalled(self):
    output_writer_spec = __name__ + ".TestOutputWriter"
    self.init(output_writer_spec=output_writer_spec)
    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    # This slice had acquired shard lock in previous attempt.
    shard_state.acquired_once = True
    shard_state.put()

    with self.app.test_request_context(
      "/mapreduce/worker_callback/" + self.shard_id,
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=self.transient_state.to_dict()
    ):
      self.handler.dispatch_request()
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        result_status=None,
        slice_id=self.slice_id+2)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, self.slice_id+2,
                           output_writer_spec=output_writer_spec)
    self.assertTrue("recover" in TestOutputWriter.events)

  def testSliceRecoveryFailed(self):
    """Test that slice recovery failures are retried like all other failures."""
    self.init(output_writer_spec=__name__ + ".TestOutputWriter")
    def _raise(self):
      raise Exception("Raise an exception on intention.")
    TestOutputWriter._supports_slice_recovery = _raise

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    # This slice had acquired shard lock in previous attempt.
    shard_state.acquired_once = True
    shard_state.put()

    with self.app.test_request_context(
      "/mapreduce/worker_callback/" + self.shard_id,
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=self.transient_state.to_dict()
    ), self.assertRaises(werkzeug.exceptions.ServiceUnavailable):
      self.handler.dispatch_request()

    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        result_status=None,
        slice_retries=1)

  def testSliceAndShardRetries(self):
    """Test when a handler throws a non fatal exception."""
    self.init(__name__ + ".fake_handler_raise_exception")
    TestEntity().put()

    # First time, the task gets retried.
    with self.assertRaises(werkzeug.exceptions.ServiceUnavailable):
      self._handle_request(expect_finalize=False)

    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        processed=0,
        slice_retries=1)

    # After the Nth attempt on slice, we retry the shard.
    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    shard_state.slice_retries = (
        parameters.config.TASK_MAX_DATA_PROCESSING_ATTEMPTS)
    shard_state.put()

    with self.app.test_request_context(
      "/mapreduce/worker_callback/" + self.shard_id,
      method="POST",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=self.transient_state.to_dict()
    ):
      self.handler.post()
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        result_status=None,
        processed=0,
        slice_retries=0,
        retries=1)

  # TODO(user): test MR jobs that only allow slice or shard retry when
  # it is configurable per job.
  def testShardRetryFailed(self):
    """Test when shard retry failed."""
    self.init(__name__ + ".fake_handler_raise_exception",
              shard_retries=parameters.config.SHARD_MAX_ATTEMPTS)
    TestEntity().put()

    # Slice attempts have exhausted.
    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    shard_state.slice_retries = (
        parameters.config.TASK_MAX_DATA_PROCESSING_ATTEMPTS)
    shard_state.put()

    self._handle_request(expect_finalize=False)
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False,
        result_status=model.ShardState.RESULT_FAILED,
        processed=1,
        slice_retries=parameters.config.TASK_MAX_DATA_PROCESSING_ATTEMPTS,
        retries=parameters.config.SHARD_MAX_ATTEMPTS)

  def testShardRetryInitiatedAtBeginning(self):
    """Test shard retry can be initiated at the beginning of a slice."""
    self.init(__name__ + ".fake_handler_yield_keys")
    TestEntity().put()

    # Slice has been attempted before.
    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    shard_state.acquired_once = True
    shard_state.put()

    # Disable slice retry.
    parameters.config.TASK_MAX_DATA_PROCESSING_ATTEMPTS = 1

    with self.app.test_request_context(
      "/mapreduce/worker_callback/" + self.shard_id,
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=self.transient_state.to_dict()
    ):
      self.handler.dispatch_request()
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        result_status=None,
        processed=0,
        slice_retries=0,
        # Retried once.
        retries=1)

  def testSuccessfulSliceRetryClearsSliceRetriesCount(self):
    self.init(__name__ + ".fake_handler_yield_op")
    TestEntity().put()
    TestEntity().put()
    # Force enqueue another task.
    parameters.config._SLICE_DURATION_SEC = 0

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    shard_state.slice_retries = (
        parameters.config.TASK_MAX_DATA_PROCESSING_ATTEMPTS - 1)
    shard_state.put()

    with self.app.test_request_context(
      "/mapreduce/worker_callback/" + self.shard_id,
      method="POST",
      headers={
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        "X-AppEngine-QueueName": "default",
        "X-Appengine-TaskName": "task_name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        util._MR_SHARD_ID_TASK_HEADER: self.shard_id
      }, 
      data=self.transient_state.to_dict()
    ):
      self.handler.dispatch_request()
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        processed=1,
        slice_retries=0)

  def testExceptionInHandler(self):
    """Test behavior when handler throws exception."""
    self.init(__name__ + ".fake_handler_raise_exception")
    TestEntity().put()

    with mock.patch.object(context.Context, "_set") as mock_set:
      with self.app.test_request_context(
        "/mapreduce/worker_callback/" + self.shard_id,
        method="POST",
        headers={
          model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
          "X-AppEngine-QueueName": "default",
          "X-Appengine-TaskName": "task_name",
          util._MR_ID_TASK_HEADER: self.mapreduce_id,
          util._MR_SHARD_ID_TASK_HEADER: self.shard_id
        }, 
        data=self.transient_state.to_dict()
      ), self.assertRaises(werkzeug.exceptions.ServiceUnavailable):
        self.handler.dispatch_request()

      # slice should be still active
      shard_state = model.ShardState.get_by_shard_id(self.shard_id)
      self.verify_shard_state(shard_state, processed=0, slice_retries=1)
      # mapper calls counter should not be incremented
      self.assertEqual(0, shard_state.counters_map.get(
          context.COUNTER_MAPPER_CALLS))

      # new task should not be spawned
      tasks = self.taskqueue.GetTasks("default")
      self.assertEqual(0, len(tasks))

      mock_set.assert_called()

  def testFailJobExceptionInHandler(self):
    """Test behavior when handler throws exception."""
    self.init(__name__ + ".fake_handler_raise_fail_job_exception")
    TestEntity().put()

    with mock.patch.object(context.Context, "_set") as mock_set:
      with self.app.test_request_context(
        "/mapreduce/worker_callback/" + self.shard_id,
        method="POST",
        headers={
          model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
          "X-AppEngine-QueueName": "default",
          "X-Appengine-TaskName": "task_name",
          util._MR_ID_TASK_HEADER: self.mapreduce_id,
          util._MR_SHARD_ID_TASK_HEADER: self.shard_id
        }, 
        data=self.transient_state.to_dict()
      ):
        self.handler.dispatch_request()

      # slice should not be active
      shard_state = model.ShardState.get_by_shard_id(self.shard_id)
      self.verify_shard_state(
          shard_state,
          processed=1,
          active=False,
          result_status = model.ShardState.RESULT_FAILED)
      self.assertEqual(1, shard_state.counters_map.get(
          context.COUNTER_MAPPER_CALLS))

      # new task should not be spawned
      tasks = self.taskqueue.GetTasks("default")
      self.assertEqual(0, len(tasks))

      mock_set.assert_called()

  def testContext(self):
    """Test proper context initialization."""
    TestEntity().put()

    with mock.patch.object(context.Context, '_set') as mock_set:
      with self.app.test_request_context(
        "/mapreduce/worker_callback/" + self.shard_id,
        method="POST",
        headers={
          model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
          "X-AppEngine-QueueName": "default",
          "X-Appengine-TaskName": "task_name",
          "X-AppEngine-TaskExecutionCount": 5,
          util._MR_ID_TASK_HEADER: self.mapreduce_id,
          util._MR_SHARD_ID_TASK_HEADER: self.shard_id
        }, 
        data=self.transient_state.to_dict()
      ):
        self.handler.dispatch_request()

      self.assertEqual(2, mock_set.call_count)
      self.assertEqual(5, mock_set.call_args_list[0][0][0].task_retry_count)
      mock_set.assert_called_with(None)

  def testContextFlush(self):
    """Test context handling."""
    TestEntity().put()

    with mock.patch.object(context.Context, "_set") as mock_set, \
         mock.patch.object(context.Context, "flush") as mock_flush:
      with self.app.test_request_context(
        "/mapreduce/worker_callback/" + self.shard_id,
        method="POST",
        headers={
          model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
          "X-AppEngine-QueueName": "default",
          "X-Appengine-TaskName": "task_name",
          util._MR_ID_TASK_HEADER: self.mapreduce_id,
          util._MR_SHARD_ID_TASK_HEADER: self.shard_id
        }, 
        data=self.transient_state.to_dict()
      ):
        self.handler.dispatch_request()

      #  1 entity should be processed
      self.assertEqual(1, len(TestHandler.processed_keys))

      mock_set.assert_called()
      mock_flush.assert_called_once()

  def testOperationYield(self):
    """Test yielding operations from handler."""
    self.init(__name__ + ".fake_handler_yield_op")
    e1 = TestEntity().put()
    e2 = TestEntity().put()

    self._handle_request()
    self.assertEqual([str(e1), str(e1), str(e2), str(e2)],
                      TestOperation.processed_keys)

  def testOutputWriter(self):
    self.init(__name__ + ".fake_handler_yield_keys",
              output_writer_spec=__name__ + ".TestOutputWriter")
    e1 = TestEntity().put()
    e2 = TestEntity().put()

    self._handle_request()

    self.assertEqual(
        ["create-1",
         "write-" + str(e1),
         "write-" + str(e2),
         "finalize-1",
        ], TestOutputWriter.events)


class ControllerCallbackHandlerTest(MapreduceHandlerTestBase):
  """Test handlers.ControllerCallbackHandler."""

  def setUp(self):
    """Sets up the test harness."""
    MapreduceHandlerTestBase.setUp(self)

    self.mapreduce_state = model.MapreduceState.create_new()
    self.mapreduce_state.put()

    self.mapreduce_id = self.mapreduce_state.key().name()
    mapreduce_spec = self.create_mapreduce_spec(self.mapreduce_id, 3)
    mapreduce_spec.params[PARAM_DONE_CALLBACK] = "/fin"
    mapreduce_spec.params[PARAM_DONE_CALLBACK_QUEUE] = "crazy-queue"
    mapreduce_spec.params["base_path"] = parameters.config.BASE_PATH

    self.mapreduce_state.mapreduce_spec = mapreduce_spec
    self.mapreduce_state.chart_url = "https://www.google.com/chart?"
    self.mapreduce_state.active = True
    self.mapreduce_state.active_shards = 3
    self.mapreduce_state.put()

    self.handler = handlers.ControllerCallbackHandler()

    self.verify_mapreduce_state(self.mapreduce_state, shard_count=3)

  def verify_done_task(self):
    tasks = self.taskqueue.GetTasks("crazy-queue")
    self.assertEqual(1, len(tasks))
    task = tasks[0]
    self.assertTrue(task)

    self.assertEqual("/fin", task["url"])
    self.assertEqual("POST", task["method"])
    headers = dict(task["headers"])
    self.assertEqual(self.mapreduce_id, headers["Mapreduce-Id"])
    self.assertEqual(self.host, headers["Host"])

  def testStateUpdateIsCmpAndSet(self):
    """Verify updating model.MapreduceState is cmp and set."""
    # Create shard states for 3 finished shards.
    shard_states = []
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.active = False
      shard_state.put()
      shard_states.append(shard_state)

    # MapreduceState.active is changed to False by another duplicated running
    # controller task.
    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    mapreduce_state.active = False
    mapreduce_state.result_status = model.MapreduceState.RESULT_SUCCESS
    mapreduce_state.put()

    # Invoke controller handler with stale mapreduce_state and shard_states.
    mapreduce_state.active = True
    mapreduce_state.result_status = None
    for s in shard_states:
      s.active = True
    self.handler._update_state_from_shard_states(mapreduce_state,
                                                 shard_states,
                                                 None)

    # Make sure we did't overwrite active or result_status.
    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state,
        shard_count=3,
        active=False,
        result_status=model.MapreduceState.RESULT_SUCCESS)

    # New controller task will drop itself because it detected that
    # mapreduce_state.active is False.
    # It will enqueue a finalizejob callback to cleanup garbage.
    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION
      },
      method="POST",
      data={
        "mapreduce_spec": mapreduce_state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()
    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    self.assertEqual("/mapreduce/finalizejob_callback/" + self.mapreduce_id,
                     tasks[0]["url"])

  def testDecodingPayloadFailed(self):
    for i in range(3):
      self.create_and_store_shard_state(self.mapreduce_id, i)

    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
      },
      method="POST",
      data={
        "mapreduce_spec": self.mapreduce_state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()

    state = model.MapreduceState.get_by_job_id(self.mapreduce_id)
    self.assertFalse(state.active)
    self.assertEqual(model.MapreduceState.RESULT_FAILED, state.result_status)

    shard_states = model.ShardState.find_all_by_mapreduce_state(state)
    for ss in shard_states:
      self.assertFalse(ss.active)
      self.assertEqual(model.ShardState.RESULT_FAILED, ss.result_status)

  def testDecodingPayloadFailedIdempotency(self):
    for i in range(3):
      self.create_and_store_shard_state(self.mapreduce_id, i)
    # Set one shard state to failed as if the drop_gracefully logic has
    # been run once but failed.
    state = model.MapreduceState.get_by_job_id(self.mapreduce_id)
    shard_states = list(model.ShardState.find_all_by_mapreduce_state(state))
    shard_states[0].set_for_failure()
    shard_states[0].put()

    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
      },
      method="POST",
      data={
        "mapreduce_spec": state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()
      self.handler.dispatch_request()

    state = model.MapreduceState.get_by_job_id(self.mapreduce_id)
    self.assertFalse(state.active)
    self.assertEqual(model.MapreduceState.RESULT_FAILED, state.result_status)

    shard_states = model.ShardState.find_all_by_mapreduce_state(state)
    for ss in shard_states:
      self.assertFalse(ss.active)
      self.assertEqual(model.ShardState.RESULT_FAILED, ss.result_status)

  def testSmoke(self):
    """Verify main execution path.

    Should aggregate all data from all shards correctly.
    """
    # check that chart_url is updated.
    self.mapreduce_state.chart_url = ""
    self.mapreduce_state.put()

    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.counters_map.increment(
          COUNTER_MAPPER_CALLS, i * 2 + 1)  # 1, 3, 5
      # We should have mapreduce active even some (not all)
      # shards are not active
      if i == 0:
        shard_state.active = False
      shard_state.put()

    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
      },
      method="POST",
      data={
        "mapreduce_spec": self.mapreduce_state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    # we should have 1 + 3 + 5 = 9 elements processed
    self.verify_mapreduce_state(mapreduce_state, processed=9, shard_count=3)
    self.assertEqual(0, mapreduce_state.failed_shards)
    self.assertEqual(0, mapreduce_state.aborted_shards)

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    for task in tasks:
      headers = dict(task["headers"])
      self.assertEqual(self.mapreduce_id, headers[util._MR_ID_TASK_HEADER])
      self.assertEqual(self.host, headers["Host"])
    self.verify_controller_task(tasks[0], shard_count=3)

  def testMissingShardState(self):
    """Correct handling of missing shard state."""
    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
      },
      method="POST",
      data={
        "mapreduce_spec": self.mapreduce_state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(mapreduce_state, active=False, shard_count=3,
                                result_status=model.ShardState.RESULT_FAILED)
    self.assertEqual(0, mapreduce_state.failed_shards)
    self.assertEqual(0, mapreduce_state.aborted_shards)

    # Abort signal should be present.
    self.assertEqual(
        model.MapreduceControl.ABORT,
        db.get(model.MapreduceControl.get_key_by_job_id(
            self.mapreduce_id)).command)

    tasks = self.taskqueue.GetTasks("default")
    # Finalize task should be spawned.
    self.assertEqual(1, len(tasks))
    self.assertEqual("/mapreduce/finalizejob_callback/" + self.mapreduce_id,
                      tasks[0]["url"])

    # Done Callback task should be spawned
    self.verify_done_task()

  def testAllShardsAreDone(self):
    """Mapreduce should become inactive when all shards have finished."""
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.counters_map.increment(
          COUNTER_MAPPER_CALLS, i * 2 + 1)  # 1, 3, 5
      shard_state.active = False
      shard_state.result_status = model.ShardState.RESULT_SUCCESS
      shard_state.put()

    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
      },
      method="POST",
      data={
        "mapreduce_spec": self.mapreduce_state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, processed=9, active=False, shard_count=3,
        result_status=model.MapreduceState.RESULT_SUCCESS)
    self.assertEqual(0, mapreduce_state.failed_shards)
    self.assertEqual(0, mapreduce_state.aborted_shards)

    tasks = self.taskqueue.GetTasks("default")
    # Finalize task should be spawned.
    self.assertEqual(1, len(tasks))
    self.assertEqual("/mapreduce/finalizejob_callback/" + self.mapreduce_id,
                      tasks[0]["url"])
    headers = dict(tasks[0]["headers"])
    self.assertEqual(self.mapreduce_id, headers[util._MR_ID_TASK_HEADER])
    self.assertEqual(self.host, headers["Host"])

    # Done Callback task should be spawned
    self.verify_done_task()

    self.assertEqual(3, len(list(
        model.ShardState.find_all_by_mapreduce_state(mapreduce_state))))

  def testShardsDoneFinalizeOutputWriter(self):
    self.mapreduce_state.mapreduce_spec.mapper.output_writer_spec = (
        __name__ + "." + TestOutputWriter.__name__)
    self.mapreduce_state.put()

    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.counters_map.increment(
          COUNTER_MAPPER_CALLS, i * 2 + 1)  # 1, 3, 5
      shard_state.active = False
      shard_state.result_status = model.ShardState.RESULT_SUCCESS
      shard_state.put()

    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
      },
      method="POST",
      data={
        "mapreduce_spec": self.mapreduce_state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()

    self.assertEqual(["finalize_job"], TestOutputWriter.events)


  def testShardsDoneWithHooks(self):
    self.mapreduce_state.mapreduce_spec.hooks_class_name = (
        __name__ + '.' + TestHooks.__name__)
    self.mapreduce_state.put()

    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.active = False
      shard_state.result_status = model.ShardState.RESULT_SUCCESS
      shard_state.put()

    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
      },
      method="POST",
      data={
        "mapreduce_spec": self.mapreduce_state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()

    self.assertEqual(1, len(TestHooks.enqueue_done_task_calls))
    task, queue_name = TestHooks.enqueue_done_task_calls[0]
    self.assertEqual('crazy-queue', queue_name)
    self.assertEqual('/fin', task.url)

  def testShardFailure(self):
    """Tests that when one shard fails the job will be aborted."""
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      if i == 0:
        shard_state.result_status = model.ShardState.RESULT_FAILED
        shard_state.active = False
      else:
        shard_state.result_status = model.ShardState.RESULT_SUCCESS
        shard_state.active = True
      shard_state.put()

    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
      },
      method="POST",
      data={
        "mapreduce_spec": self.mapreduce_state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, active=True, shard_count=3)
    self.assertEqual(1, mapreduce_state.failed_shards)
    self.assertEqual(0, mapreduce_state.aborted_shards)

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    self.verify_controller_task(tasks[0], shard_count=3)

    # Abort signal should be present.
    self.assertEqual(
        model.MapreduceControl.ABORT,
        db.get(model.MapreduceControl.get_key_by_job_id(
            self.mapreduce_id)).command)

  def testShardFailureAllDone(self):
    """Tests that individual shard failure affects the job outcome."""
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.active = False
      if i == 0:
        shard_state.result_status = model.ShardState.RESULT_FAILED
      elif i == 1:
        shard_state.result_status = model.ShardState.RESULT_ABORTED
      else:
        shard_state.result_status = model.ShardState.RESULT_SUCCESS
      shard_state.put()

    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
      },
      method="POST",
      data={
        "mapreduce_spec": self.mapreduce_state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, active=False, shard_count=3,
        result_status=model.ShardState.RESULT_FAILED)
    self.assertEqual(1, mapreduce_state.failed_shards)
    self.assertEqual(1, mapreduce_state.aborted_shards)

    tasks = self.taskqueue.GetTasks("default")
    # Finalize task should be spawned.
    self.assertEqual(1, len(tasks))
    self.assertEqual("/mapreduce/finalizejob_callback/" + self.mapreduce_id,
                      tasks[0]["url"])

    # Done Callback task should be spawned
    self.verify_done_task()

    self.assertEqual(3, len(list(
        model.ShardState.find_all_by_mapreduce_state(mapreduce_state))))

  def testUserAbort(self):
    """Tests that user abort will stop the job."""
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.active = True
      shard_state.put()

    model.MapreduceControl.abort(self.mapreduce_id)
    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
      },
      method="POST",
      data={
        "mapreduce_spec": self.mapreduce_state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()
    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, active=True, shard_count=3)
    self.assertEqual(0, mapreduce_state.failed_shards)
    self.assertEqual(0, mapreduce_state.aborted_shards)

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    self.verify_controller_task(tasks[0], shard_count=3)
    self.taskqueue.FlushQueue("default")

    # Repeated calls to callback closure while the shards are active will
    # result in a no op. As the controller waits for the shards to finish.
    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
      },
      method="POST",
      data={
        "mapreduce_spec": self.mapreduce_state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()
    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, active=True, shard_count=3)
    self.assertEqual(0, mapreduce_state.failed_shards)
    self.assertEqual(0, mapreduce_state.aborted_shards)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    self.verify_controller_task(tasks[0], shard_count=3)
    self.taskqueue.FlushQueue("default")

    # Force all shards to completion state (success, success, or abort).
    shard_state_list = list(
        model.ShardState.find_all_by_mapreduce_state(mapreduce_state))
    self.assertEqual(3, len(shard_state_list))
    shard_state_list[0].active = False
    shard_state_list[0].result_status = model.ShardState.RESULT_SUCCESS
    shard_state_list[1].active = False
    shard_state_list[1].result_status = model.ShardState.RESULT_SUCCESS
    shard_state_list[2].active = False
    shard_state_list[2].result_status = model.ShardState.RESULT_ABORTED
    db.put(shard_state_list)

    with self.app.test_request_context(
      "/mapreduce/controller_callback",
      headers={
        "X-AppEngine-QueueName": "default",
        "X-AppEngine-TaskName": "foo-task-name",
        util._MR_ID_TASK_HEADER: self.mapreduce_id,
        model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
      },
      method="POST",
      data={
        "mapreduce_spec": self.mapreduce_state.mapreduce_spec.to_json_str(),
        "serial_id": "1234",
      },
    ):
      self.handler.dispatch_request()
    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, active=False, shard_count=3,
        result_status=model.MapreduceState.RESULT_ABORTED)
    self.assertEqual(1, mapreduce_state.aborted_shards)

    tasks = self.taskqueue.GetTasks("default")
    # Finalize task should be spawned.
    self.assertEqual(1, len(tasks))
    self.assertEqual("/mapreduce/finalizejob_callback/" + self.mapreduce_id,
                      tasks[0]["url"])

    # Done Callback task should be spawned
    self.verify_done_task()

  def testScheduleQueueName(self):
    """Tests that the calling queue name is preserved on schedule calls."""
    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = "crazy-queue"
    try:
      self.mapreduce_state.put()
      for i in range(3):
        shard_state = self.create_shard_state(self.mapreduce_id, i)
        shard_state.put()

      with self.app.test_request_context(
        "/mapreduce/controller_callback",
        headers={
          "X-AppEngine-QueueName": "default",
          "X-AppEngine-TaskName": "foo-task-name",
          util._MR_ID_TASK_HEADER: self.mapreduce_id,
          model.HugeTask.PAYLOAD_VERSION_HEADER: model.HugeTask.PAYLOAD_VERSION,
        },
        method="POST",
        data={
          "mapreduce_spec": self.mapreduce_state.mapreduce_spec.to_json_str(),
          "serial_id": "1234",
        },
      ):
        self.handler.dispatch_request()

      # new task should be spawned on the calling queue
      tasks = self.taskqueue.GetTasks("crazy-queue")
      self.assertEqual(1, len(tasks))
      self.verify_controller_task(tasks[0], shard_count=3)
    finally:
      del os.environ["HTTP_X_APPENGINE_QUEUENAME"]


class CleanUpJobTest(testutil.HandlerTestBase):
  """Tests cleaning up jobs."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)

    TestKind().put()
    self.mapreduce_id = control.start_map(
        "my job 1",
        f"{TestMap.__module__}.TestMap",
        "mapreduce.input_readers.DatastoreInputReader",
        {"entity_kind": f"{TestKind.__module__}.TestKind"},
        4)

    self.handler = handlers.CleanUpJobHandler()

  def KickOffMapreduce(self):
    """Executes pending kickoff task."""
    test_support.execute_all_tasks(self.taskqueue)

  def testCSRF(self):
    """Test that we check the X-Requested-With header."""
    with self.app.test_request_context():
      with self.assertRaises(werkzeug.exceptions.Forbidden):
        self.handler.post()

  def testBasic(self):
    """Tests cleaning up the job.

    Note: This cleans up a running mapreduce, but that's okay because
    the prohibition against doing so is done on the client side.
    """
    self.KickOffMapreduce()
    key = model.MapreduceState.get_key_by_job_id(self.mapreduce_id)
    self.assertTrue(db.get(key))

    with self.app.test_request_context(
        headers={"X-Requested-With": "XMLHttpRequest"},
        query_string={"mapreduce_id": self.mapreduce_id}
    ):
      response = self.handler.post()

    result = json.loads(response.get_data(as_text=True))
    self.assertEqual({"status": ("Job %s successfully cleaned up." %
                                  self.mapreduce_id) },
                      result)

    state = model.MapreduceState.get_by_job_id(self.mapreduce_id)
    self.assertFalse(state)
    self.assertFalse(list(model.ShardState.find_all_by_mapreduce_state(state)))

