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




# Disable "Invalid method name"
# pylint: disable=g-bad-name

import datetime
import os
import types
import unittest
from unittest import mock
import urllib.parse

from google.appengine.ext import db
from google.appengine.ext import testbed
from mapreduce import hooks
from mapreduce import model


class FakeHandler:
  """Test handler class."""

  def __call__(self, entity):
    pass

  def process(self, entity):
    pass


class FakeHandlerWithArgs:
  """Test handler with argument in constructor."""

  def __init__(self, arg_unused):
    """Constructor."""
    pass

  def process(self, entity):
    """Empty process function."""
    pass


class FakeHooks(hooks.Hooks):
  """Test hooks class."""
  pass


def fake_handler_function(entity):
  """Empty test handler function."""
  pass


class HugeTaskTest(unittest.TestCase):
  """HugeTask tests.

  Other tests are in end_to_end_test.py
  """

  def testIncorrectPayloadVersion(self):
    request = mock.Mock()
    request.headers = {}
    self.assertRaises(DeprecationWarning,
                      model.HugeTask.decode_payload,
                      request)
    request.headers[model.HugeTask.PAYLOAD_VERSION_HEADER] = "0"
    self.assertRaises(DeprecationWarning,
                      model.HugeTask.decode_payload,
                      request)


class TestReader:
  pass


class TestWriter:
  pass


class MapperSpecTest(unittest.TestCase):
  """Tests model.MapperSpec."""

  ENTITY_KIND = "__main__.TestEntity"
  TEST_HANDLER = __name__ + "." + FakeHandler.__name__
  TEST_READER = __name__ + "." + TestReader.__name__
  TEST_WRITER = __name__ + "." + TestWriter.__name__

  def setUp(self):
    self.default_json = {
        "mapper_handler_spec": self.TEST_HANDLER,
        "mapper_input_reader": self.TEST_READER,
        "mapper_params": {"entity_kind": self.ENTITY_KIND},
        "mapper_shard_count": 8}

  def testToJson(self):
    mapper_spec = model.MapperSpec(
        self.TEST_HANDLER,
        self.TEST_READER,
        {"entity_kind": self.ENTITY_KIND},
        8)
    self.assertEqual(self.default_json,
                      mapper_spec.to_json())

    mapper_spec = model.MapperSpec(
        self.TEST_HANDLER,
        self.TEST_READER,
        {"entity_kind": self.ENTITY_KIND},
        8,
        output_writer_spec=self.TEST_WRITER)
    d = dict(self.default_json)
    d["mapper_output_writer"] = self.TEST_WRITER
    self.assertEqual(d, mapper_spec.to_json())

  def testFromJson(self):
    ms = model.MapperSpec.from_json(self.default_json)
    self.assertEqual(self.TEST_READER, ms.input_reader_spec)
    self.assertEqual(TestReader, ms.input_reader_class())
    self.assertEqual(self.default_json["mapper_input_reader"],
                      ms.input_reader_spec)
    self.assertEqual(self.TEST_HANDLER, ms.handler_spec)
    self.assertTrue(isinstance(ms.get_handler(), FakeHandler))
    self.assertTrue(isinstance(ms.handler, FakeHandler))
    self.assertEqual(8, ms.shard_count)

    d = dict(self.default_json)
    d["mapper_output_writer"] = self.TEST_WRITER
    ms = model.MapperSpec.from_json(d)
    self.assertEqual(self.TEST_WRITER, ms.output_writer_spec)
    self.assertEqual(TestWriter, ms.output_writer_class())

  def specForHandler(self, handler_spec):
    self.default_json["mapper_handler_spec"] = handler_spec
    return model.MapperSpec.from_json(self.default_json)

  def testClassHandler(self):
    """Test class name as handler spec."""
    mapper_spec = self.specForHandler(
        __name__ + "." + FakeHandler.__name__)
    self.assertTrue(FakeHandler,
                    type(mapper_spec.handler))

  def testInstanceMethodHandler(self):
    """Test instance method as handler spec."""
    mapper_spec = self.specForHandler(f"{FakeHandler.__module__}.{FakeHandler.__name__}.process")
    self.assertEqual(types.FunctionType,
                      type(mapper_spec.handler))
    mapper_spec.handler(0, None)

  def testFunctionHandler(self):
    """Test function name as handler spec."""
    mapper_spec = self.specForHandler(
        __name__ + "." + fake_handler_function.__name__)
    self.assertEqual(types.FunctionType,
                      type(mapper_spec.handler))
    mapper_spec.handler(0)

  def testHandlerWithConstructorArgs(self):
    """Test class with constructor args as a handler."""
    mapper_spec = self.specForHandler(
        __name__ + "." + FakeHandlerWithArgs.__name__)
    self.assertRaises(TypeError, mapper_spec.get_handler)

  def testMethodHandlerWithConstructorArgs(self):
    """Test method from a class with constructor args as a handler."""
    mapper_spec = self.specForHandler(
      f"{__name__}.{FakeHandlerWithArgs.__name__}.process")
    with self.assertRaises(TypeError):
      handler = mapper_spec.get_handler()
      handler()


class MapreduceSpecTest(unittest.TestCase):
  """Tests model.MapreduceSpec."""

  def testToJson(self):
    """Test to_json method."""
    mapper_spec_dict = {"mapper_handler_spec": "TestHandler",
                        "mapper_input_reader": "TestInputReader",
                        "mapper_params": {"entity_kind": "bar"},
                        "mapper_shard_count": 8}
    mapreduce_spec = model.MapreduceSpec("my job",
                                         "mr0",
                                         mapper_spec_dict,
                                         {"extra": "value"},
                                         __name__+"."+FakeHooks.__name__)
    self.assertEqual(
        {"name": "my job",
         "mapreduce_id": "mr0",
         "mapper_spec": mapper_spec_dict,
         "params": {"extra": "value"},
         "hooks_class_name": __name__+"."+FakeHooks.__name__,
        },
        mapreduce_spec.to_json())

  def testFromJsonWithoutOptionalArgs(self):
    """Test from_json method without params and hooks_class_name present."""
    mapper_spec_dict = {"mapper_handler_spec": "TestHandler",
                        "mapper_input_reader": "TestInputReader",
                        "mapper_params": {"entity_kind": "bar"},
                        "mapper_shard_count": 8}
    mapreduce_spec = model.MapreduceSpec.from_json(
        {"mapper_spec": mapper_spec_dict,
         "mapreduce_id": "mr0",
         "name": "my job",
        })

    self.assertEqual("my job", mapreduce_spec.name)
    self.assertEqual("mr0", mapreduce_spec.mapreduce_id)
    self.assertEqual(mapper_spec_dict, mapreduce_spec.mapper.to_json())
    self.assertEqual("TestHandler", mapreduce_spec.mapper.handler_spec)
    self.assertEqual(None, mapreduce_spec.params)
    self.assertEqual(None, mapreduce_spec.hooks_class_name)

  def testFromJsonWithOptionalArgs(self):
    """Test from_json method with params and hooks_class_name present."""
    mapper_spec_dict = {"mapper_handler_spec": "TestHandler",
                        "mapper_input_reader": "TestInputReader",
                        "mapper_params": {"entity_kind": "bar"},
                        "mapper_shard_count": 8}
    mapreduce_spec = model.MapreduceSpec.from_json(
        {"mapper_spec": mapper_spec_dict,
         "mapreduce_id": "mr0",
         "name": "my job",
         "params": {"extra": "value"},
         "hooks_class_name": __name__+"."+FakeHooks.__name__
        })

    self.assertEqual("my job", mapreduce_spec.name)
    self.assertEqual("mr0", mapreduce_spec.mapreduce_id)
    self.assertEqual(mapper_spec_dict, mapreduce_spec.mapper.to_json())
    self.assertEqual("TestHandler", mapreduce_spec.mapper.handler_spec)
    self.assertEqual({"extra": "value"}, mapreduce_spec.params)
    self.assertEqual(__name__+"."+FakeHooks.__name__,
                      mapreduce_spec.hooks_class_name)
    self.assertEqual(mapreduce_spec, mapreduce_spec.get_hooks().mapreduce_spec)


class ShardStateTest(unittest.TestCase):
  """Tests model.ShardState."""

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()

  def tearDown(self):
    self.testbed.deactivate()

  def testAccessors(self):
    """Tests simple accessors."""
    shard = model.ShardState.create_new("my-map-job1", 14)
    self.assertEqual(14, shard.shard_number)

  def testCopyFrom(self):
    """Test copy_from method."""
    state = model.ShardState.create_new("my-map-job1", 14)
    state.active = False
    state.counters_map.increment("foo", 2)
    state.result_status = "failed"
    state.mapreduce_id = "mapreduce_id"
    state.update_time = datetime.datetime.now()
    state.shard_description = "shard_description"
    state.last_work_item = "last_work_item"

    another_state = model.ShardState.create_new("my-map-job1", 14)
    another_state.copy_from(state)
    self.assertEqual(state.active, another_state.active)
    self.assertEqual(state.counters_map, another_state.counters_map)
    self.assertEqual(state.result_status, another_state.result_status)
    self.assertEqual(state.mapreduce_id, another_state.mapreduce_id)
    self.assertEqual(state.update_time, another_state.update_time)
    self.assertEqual(state.shard_description, another_state.shard_description)
    self.assertEqual(state.last_work_item, another_state.last_work_item)

  def testFindAllByMapreduceState(self):
    mr_state = model.MapreduceState.create_new("mapreduce-id")
    mr_state.mapreduce_spec = model.MapreduceSpec(
        "mapreduce", "mapreduce-id",
        model.MapperSpec("handler", "input-reader",
                         {}, shard_count=304).to_json())
    mr_state.put()
    for i in range(304):
      model.ShardState.create_new("mapreduce-id", i).put()
    @db.transactional(xg=False)
    def non_xg_tx():
      # Open a single non-related entity group to ensure
      # find_all_by_mapreduce_state does not attempt to use outer transaction
      mr_state2 = model.MapreduceState.create_new("unrelated-mapreduce-id")
      mr_state2.put()
      shard_states = model.ShardState.find_all_by_mapreduce_state(mr_state)
      for i, ss in enumerate(shard_states):
        self.assertEqual(i, ss.shard_number)
    non_xg_tx()


class CountersMapTest(unittest.TestCase):
  """Tests model.CountersMap."""

  def testIncrementCounter(self):
    """Test increment_counter method."""
    countres_map = model.CountersMap()

    self.assertEqual(0, countres_map.get("counter1"))
    self.assertEqual(10, countres_map.increment("counter1", 10))
    self.assertEqual(10, countres_map.get("counter1"))
    self.assertEqual(20, countres_map.increment("counter1", 10))
    self.assertEqual(20, countres_map.get("counter1"))

  def testAddSubMap(self):
    """Test add_map and sub_map methods."""
    map1 = model.CountersMap()
    map1.increment("1", 5)
    map1.increment("2", 7)

    map2 = model.CountersMap()
    map2.increment("2", 8)
    map2.increment("3", 11)

    map1.add_map(map2)

    self.assertEqual(5, map1.get("1"))
    self.assertEqual(15, map1.get("2"))
    self.assertEqual(11, map1.get("3"))

    map1.sub_map(map2)

    self.assertEqual(5, map1.get("1"))
    self.assertEqual(7, map1.get("2"))
    self.assertEqual(0, map1.get("3"))

  def testToJson(self):
    """Test to_json method."""
    counters_map = model.CountersMap()
    counters_map.increment("1", 5)
    counters_map.increment("2", 7)

    self.assertEqual({"counters": {"1": 5, "2": 7}}, counters_map.to_json())

  def testFromJson(self):
    """Test from_json method."""
    counters_map = model.CountersMap()
    counters_map.increment("1", 5)
    counters_map.increment("2", 7)

    counters_map = model.CountersMap.from_json(counters_map.to_json())

    self.assertEqual(5, counters_map.get("1"))
    self.assertEqual(7, counters_map.get("2"))

  def testClear(self):
    """Test clear method."""
    counters_map = model.CountersMap()
    counters_map.increment("1", 5)
    counters_map.clear()

    self.assertEqual(0, counters_map.get("1"))


