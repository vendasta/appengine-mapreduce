#!/usr/bin/env python
# Copyright 2010 Google Inc. All Rights Reserved.
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

import datetime
import os
import sys
import unittest

from google.appengine.api import taskqueue

from mapreduce import model
from mapreduce import parameters
from mapreduce import util


class FakeHandler:
  """Test handler class."""

  def __call__(self, entity):
    pass

  def process(self, entity):
    pass

  @staticmethod
  def process2(entity):
    pass

  @classmethod
  def process3(cls):
    pass


# pylint: disable=unused-argument
def fake_handler_function(entity):
  """Empty test handler function."""
  pass


class FakeHandlerWithArgs:
  """Test handler with argument in constructor."""

  def __init__(self, arg_unused):
    """Constructor."""
    pass

  def process(self, entity):
    """Empty process function."""
    pass


# pylint: disable=g-old-style-class
class TestHandlerOldStyle():
  """Old style class."""

  def __call__(self, entity):
    pass


# pylint: disable=unused-argument
def fake_handler_yield(entity):
  """Yielding handler function."""
  yield 1
  yield 2


class MockMapreduceSpec:
  """Mock MapreduceSpec class."""

  def __init__(self):
    self.params = {}


class ForNameTest(unittest.TestCase):
  """Test util.for_name function."""

  def testClassName(self):
    """Test passing fq class name."""
    self.assertEqual(FakeHandler, util.for_name(f"{FakeHandler.__module__}.FakeHandler"))

  def testFunctionName(self):
    """Test passing function name."""
    self.assertEqual(fake_handler_function,
                      util.for_name(f"{fake_handler_function.__module__}.fake_handler_function"))

  def testMethodName(self):
    """Test passing method name."""
    self.assertEqual(FakeHandler.process,
                      util.for_name(f"{FakeHandler.__module__}.FakeHandler.process"))

  def testClassWithArgs(self):
    """Test passing method name of class with constructor args."""
    self.assertEqual(FakeHandlerWithArgs.process,
                      util.for_name(f"{FakeHandlerWithArgs.__module__}.FakeHandlerWithArgs.process"))

  def testBadModule(self):
    """Tests when the module name is bogus."""
    try:
      util.for_name("this_is_a_bad_module_name.stuff")
    except ImportError as e:
      self.assertEqual(
          "Could not find 'stuff' on path 'this_is_a_bad_module_name'",
          str(e))
    else:
      self.fail("Did not raise exception")

  def testBadFunction(self):
    """Tests when the module name is good but the function is missing."""
    try:
      util.for_name("__main__.does_not_exist")
    except ImportError as e:
      self.assertEqual(
          "Could not find 'does_not_exist' on path '__main__'",
          str(e))
    else:
      self.fail("Did not raise exception")

  def testBadClass(self):
    """Tests when the class is found but the function name is missing."""
    try:
      util.for_name("__main__.TestHandlerWithArgs.missing")
    except ImportError as e:
      self.assertEqual(
          "Could not find 'missing' on path '__main__.TestHandlerWithArgs'",
          str(e))
    else:
      self.fail("Did not raise exception")

  def testGlobalName(self):
    """Tests when the name has no dots in it."""
    try:
      util.for_name("this_is_a_bad_module_name")
    except ImportError as e:
      self.assertTrue(str(e).startswith(
          "Could not find 'this_is_a_bad_module_name' on path "))
    else:
      self.fail("Did not raise exception")


class TestGetQueueName(unittest.TestCase):

  def testGetQueueName(self):
    self.assertEqual("foo", util.get_queue_name("foo"))

    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = "foo"
    self.assertEqual("foo", util.get_queue_name(None))

    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = "__cron"
    self.assertEqual(parameters.config.QUEUE_NAME, util.get_queue_name(None))


class SerializeHandlerTest(unittest.TestCase):
  """Test util.try_*serialize_handler works on various types."""

  def testNonSerializableTypes(self):
    # function.
    self.assertEqual(None, util.try_serialize_handler(fake_handler_function))
    # Unbound method.
    self.assertEqual(None, util.try_serialize_handler(FakeHandler.process))
    # bounded method.
    self.assertEqual(None, util.try_serialize_handler(FakeHandler().process))
    # class method.
    self.assertEqual(None, util.try_serialize_handler(FakeHandler.process3))
    # staticmethod, which is really a function.
    self.assertEqual(None, util.try_serialize_handler(FakeHandler.process2))

  def testSerializableTypes(self):
    # new style callable instance.
    i = FakeHandler()
    self.assertNotEqual(
        None, util.try_deserialize_handler(util.try_serialize_handler(i)))

    i = TestHandlerOldStyle()
    self.assertNotEqual(
        None, util.try_deserialize_handler(util.try_serialize_handler(i)))


class IsGeneratorFunctionTest(unittest.TestCase):
  """Test util.is_generator function."""

  def testGenerator(self):
    self.assertTrue(util.is_generator(fake_handler_yield))

  def testNotGenerator(self):
    self.assertFalse(util.is_generator(fake_handler_function))


class GetTaskHeadersTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    os.environ["GAE_VERSION"] = "v7.1"
    os.environ["GAE_SERVICE"] = "foo-module"
    os.environ["DEFAULT_VERSION_HOSTNAME"] = "foo.appspot.com"

  def testGetTaskHost(self):
    self.assertEqual("v7.foo-module.foo.appspot.com", util._get_task_host())
    task = taskqueue.Task(url="/relative_url",
                          headers={"Host": util._get_task_host()})
    self.assertEqual("v7.foo-module.foo.appspot.com",
                     task.headers["Host"])
    self.assertEqual("v7.foo-module", task.target)

  def testGetTaskHostDefaultModule(self):
    os.environ["GAE_SERVICE"] = "default"
    self.assertEqual("v7.foo.appspot.com", util._get_task_host())
    task = taskqueue.Task(url="/relative_url",
                          headers={"Host": util._get_task_host()})
    self.assertEqual("v7.foo.appspot.com",
                     task.headers["Host"])
    self.assertEqual("v7", task.target)

  def testGetTaskHeaders(self):
    mr_spec = model.MapreduceSpec(
        name="foo", mapreduce_id="foo_id",
        mapper_spec=model.MapperSpec("foo", "foo", {}, 8).to_json())
    task = taskqueue.Task(url="/relative_url",
                          headers=util._get_task_headers(mr_spec.mapreduce_id))
    self.assertEqual("foo_id", task.headers[util._MR_ID_TASK_HEADER])
    self.assertEqual("v7.foo-module.foo.appspot.com",
                     task.headers["Host"])
    self.assertEqual("v7.foo-module", task.target)


class GetShortNameTest(unittest.TestCase):
  """Test util.get_short_name function."""

  def testGetShortName(self):
    self.assertEqual("blah", util.get_short_name("blah"))
    self.assertEqual("blah", util.get_short_name(".blah"))
    self.assertEqual("blah", util.get_short_name("__mmm__.blah"))
    self.assertEqual("blah", util.get_short_name("__mmm__.Krb.blah"))


class TotalSecondsTest(unittest.TestCase):
  """Test util.total_seconds."""

  def testTotalSeconds(self):
    td = datetime.timedelta(days=1, seconds=1)
    self.assertEqual(24 * 60 * 60 + 1, util.total_seconds(td))

    td = datetime.timedelta(days=1, seconds=1, microseconds=1)
    self.assertEqual(24 * 60 * 60 + 2, util.total_seconds(td))


class ParseBoolTest(unittest.TestCase):
  """Test util.parse_bool function."""

  def testParseBool(self):
    self.assertEqual(True, util.parse_bool(True))
    self.assertEqual(False, util.parse_bool(False))
    self.assertEqual(True, util.parse_bool("True"))
    self.assertEqual(False, util.parse_bool("False"))
    self.assertEqual(True, util.parse_bool(1))
    self.assertEqual(False, util.parse_bool(0))
    self.assertEqual(True, util.parse_bool("on"))
    self.assertEqual(False, util.parse_bool("off"))


class CreateConfigTest(unittest.TestCase):
  """Test create_datastore_write_config function."""

  def setUp(self):
    super().setUp()
    self.spec = MockMapreduceSpec()

  def testDefaultConfig(self):
    config = util.create_datastore_write_config(self.spec)
    self.assertTrue(config)
    self.assertFalse(config.force_writes)

  def testForceWrites(self):
    self.spec.params["force_writes"] = "True"
    config = util.create_datastore_write_config(self.spec)
    self.assertTrue(config)
    self.assertTrue(config.force_writes)


class FooClass:
  pass


class ObjToPathTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.sys_modules = sys.modules

  def tearDown(self):
    super().tearDown()
    sys.modules = self.sys_modules

  def testBasic(self):
    self.assertEqual(None, util._obj_to_path(None))
    self.assertEqual(f"{FooClass.__module__}.FooClass", util._obj_to_path(FooClass))
    self.assertEqual(f"{fake_handler_function.__module__}.fake_handler_function",
                     util._obj_to_path(fake_handler_function))

  @staticmethod
  def foo():
    pass

  class FooClass2:
    pass

  def testNotTopLevel(self):
    self.assertRaises(ValueError, util._obj_to_path, self.FooClass2)

  def testNotTopLevel2(self):
    self.assertRaises(ValueError, util._obj_to_path, self.foo)

  def testUnexpectedType(self):
    self.assertRaises(TypeError, util._obj_to_path, self.testUnexpectedType)


class GetDescendingKeyTest(unittest.TestCase):
  """Tests the _get_descending_key function."""

  def testBasic(self):
    """Basic test of the function."""
    now = 1234567890
    os.environ["REQUEST_ID_HASH"] = "12345678"

    self.assertEqual(
        "159453012940012345678",
        util._get_descending_key(
            gettime=lambda: now))


class StripPrefixFromItemsTest(unittest.TestCase):
  """Tests the strip_prefix_from_items function."""

  def testBasic(self):
    """Basic test of the function."""
    items = ["/foo/bar", "/foos/bar2", "/bar3"]
    prefix = "/foo/"

    self.assertEqual(["bar", "/foos/bar2", "/bar3"],
                      util.strip_prefix_from_items(prefix, items))


