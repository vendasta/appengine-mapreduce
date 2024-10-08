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

import random
import unittest
from unittest import mock

from google.appengine.ext import ndb
from google.appengine.api import datastore
from google.appengine.ext import db
from google.appengine.runtime import apiproxy_errors

from testlib import testutil
from mapreduce import context

# pylint: disable=g-bad-name


class FakeEntity(db.Model):
  """Test entity class to test db operations."""

  tag = db.TextProperty()


class NdbFakeEntity(ndb.Model):
  """Test entity class to test ndb operations."""

  tag = ndb.TextProperty()


def new_datastore_entity(key_name=None):
  return datastore.Entity('FakeEntity', name=key_name)


class FlushFunction:
  """Flush function to use for tests."""

  # How many time to raise timeouts.
  timeouts = 0
  # Records calls to flush function.
  calls = []
  # Where things are flushed to.
  persistent_storage = []

  def flush_function(self, items, options):
    FlushFunction.calls.append((items, dict(options)))
    if len(FlushFunction.calls) <= FlushFunction.timeouts:
      raise db.Timeout()
    FlushFunction.persistent_storage.extend(items)

  # pylint: disable=unused-argument
  def flush_function_too_large_error(self, *args, **kwds):
    raise apiproxy_errors.RequestTooLargeError()

  @classmethod
  def reset(cls):
    cls.timeouts = False
    cls.calls = []
    cls.persistent_storage = []


class ItemListTest(unittest.TestCase):
  """Tests for context._ItemList class."""

  def setUp(self):
    FlushFunction.reset()
    self.max_entity_count = 9
    self.list = context._ItemList(
        self.max_entity_count,
        FlushFunction().flush_function)

  def testShouldFlush(self):
    for _ in range(self.max_entity_count):
      self.assertFalse(self.list.should_flush())
      self.list.append('a')
    self.assertTrue(self.list.should_flush())

  def testClear(self):
    """Test clear method."""
    self.list.append('abc')
    self.list.clear()
    self.assertFieldsMatch([])

  def testInitialState(self):
    self.assertFieldsMatch([])

  def testAppend(self):
    """Test append method."""
    self.list.append('abc')
    self.list.append('de')
    self.assertFieldsMatch(['abc', 'de'])

  def testAppendTooManyEntities(self):
    items = ['a' for _ in range(self.max_entity_count)]
    for item in items:
      self.list.append(item)
    self.list.append('b')
    self.assertFieldsMatch(['b'])
    self.assertEqual(items, FlushFunction.persistent_storage)

  def testFlushWithTooLargeRequestError(self):
    self.list = context._ItemList(
        self.max_entity_count,
        FlushFunction().flush_function_too_large_error,
        repr_function=lambda item: item)
    items = [(s, 'a'*s) for s in range(10, 1, -1)]
    items_copy = list(items)
    random.seed(1)
    random.shuffle(items_copy)
    for _, i in items_copy:
      self.list.append(i)
    self.assertRaises(apiproxy_errors.RequestTooLargeError,
                      self.list.flush)
    self.assertEqual(items[:context._ItemList._LARGEST_ITEMS_TO_LOG],
                     self.list._largest)

  def testFlushRetry(self):
    FlushFunction.timeouts = context._ItemList.DEFAULT_RETRIES
    self.list.append('abc')
    self.list.flush()
    self.assertEqual(['abc'], FlushFunction.persistent_storage)
    self.assertEqual(context._ItemList.DEFAULT_RETRIES + 1,
                     len(FlushFunction.calls))
    deadlines = [t[1]['deadline'] for t in FlushFunction.calls]
    expected_deadlines = [context.DATASTORE_DEADLINE]
    for _ in range(3):
      expected_deadlines.append(expected_deadlines[-1]*2)
    self.assertEqual(expected_deadlines, deadlines)

  def testFlushTimeoutTooManyTimes(self):
    FlushFunction.timeouts = context._ItemList.DEFAULT_RETRIES + 1
    self.list.append('abc')
    self.assertRaises(db.Timeout, self.list.flush)
    self.assertEqual(context._ItemList.DEFAULT_RETRIES + 1,
                     len(FlushFunction.calls))

  def assertFieldsMatch(self, items, item_list=None):
    """Assert all internal fields are consistent."""
    if item_list is None:
      item_list = self.list
    self.assertEqual(items, item_list.items)


class MutationPoolTest(testutil.HandlerTestBase):
  """Tests for context._MutationPool class."""

  def setUp(self):
    super().setUp()
    self.pool = context._MutationPool()

  def testPoolWithForceWrites(self):
    class MapreduceSpec:
      def __init__(self):
        self.params = {'force_ops_writes':True}
    pool = context._MutationPool(mapreduce_spec=MapreduceSpec())
    self.assertTrue(pool.force_writes)

  def testPutEntity(self):
    """Test put method."""
    e = new_datastore_entity()
    self.pool.put(e)
    self.assertEqual([e], self.pool.puts.items)

    # Mix in a model instance.
    # Model instance is "normalized", meaning internal fields are populated
    # in order to accurately calculate size.
    e2 = FakeEntity()
    self.pool.put(e2)
    self.assertEqual([e, context._normalize_entity(e2)], self.pool.puts.items)

    self.assertEqual([], self.pool.deletes.items)
    self.assertEqual([], self.pool.ndb_puts.items)
    self.assertEqual([], self.pool.ndb_deletes.items)

  def testDeleteEntity(self):
    """Test delete method."""
    # Model instance.
    e1 = FakeEntity(key_name='goingaway')
    self.pool.delete(e1)
    # Datastore instance.
    e2 = new_datastore_entity(key_name='goingaway')
    self.pool.delete(e2)
    # Key.
    k = db.Key.from_path('MyKind', 'MyKeyName', _app='myapp')
    self.pool.delete(k)
    # String of key.
    self.pool.delete(str(k))

    self.assertEqual([e1.key(), e2.key(), k, k], self.pool.deletes.items)
    self.assertEqual([], self.pool.puts.items)
    self.assertEqual([], self.pool.ndb_puts.items)
    self.assertEqual([], self.pool.ndb_deletes.items)

  def testPutNdbEntity(self):
    """Test put() using an NDB entity."""
    e = NdbFakeEntity()
    self.pool.put(e)
    self.assertEqual([e], self.pool.ndb_puts.items)
    self.assertEqual([], self.pool.ndb_deletes.items)
    self.assertEqual([], self.pool.puts.items)
    self.assertEqual([], self.pool.deletes.items)

  def testDeleteNdbEntity(self):
    """Test delete method with an NDB model instance."""
    e = NdbFakeEntity(id='goingaway')
    self.pool.delete(e)
    self.assertEqual([], self.pool.ndb_puts.items)
    self.assertEqual([e.key], self.pool.ndb_deletes.items)
    self.assertEqual([], self.pool.puts.items)
    self.assertEqual([], self.pool.deletes.items)

  def testDeleteNdbKey(self):
    """Test delete method with an NDB key."""
    e = NdbFakeEntity(id='goingaway')
    self.pool.delete(e.key)
    self.assertEqual([], self.pool.ndb_puts.items)
    self.assertEqual([e.key], self.pool.ndb_deletes.items)
    self.assertEqual([], self.pool.puts.items)
    self.assertEqual([], self.pool.deletes.items)

  def testFlush(self):
    """Combined test for all db implicit and explicit flushing."""
    self.pool = context._MutationPool(max_entity_count=3)

    for i in range(8):
      self.pool.put(FakeEntity())
      self.assertEqual(len(self.pool.puts.items), (i%3) + 1)

    for i in range(5):
      e = FakeEntity()
      e.put()
      self.pool.delete(e)
      self.assertEqual(len(self.pool.deletes.items), (i%3) + 1)

    self.pool.flush()
    self.assertEqual(len(self.pool.puts.items), 0)
    self.assertEqual(len(self.pool.deletes.items), 0)
    self.assertEqual(len(self.pool.ndb_puts.items), 0)
    self.assertEqual(len(self.pool.ndb_deletes.items), 0)

  def testNdbFlush(self):
    """Combined test for all NDB implicit and explicit flushing."""
    self.pool = context._MutationPool(max_entity_count=3)

    for i in range(8):
      self.pool.put(NdbFakeEntity())
      self.assertEqual(len(self.pool.ndb_puts.items), (i%3) + 1)

    for i in range(5):
      self.pool.delete(ndb.Key(NdbFakeEntity, 'x%d' % i))
      self.assertEqual(len(self.pool.ndb_deletes.items), (i%3) + 1)

    self.pool.flush()
    self.assertEqual(len(self.pool.ndb_puts.items), 0)
    self.assertEqual(len(self.pool.ndb_deletes.items), 0)
    self.assertEqual(len(self.pool.puts.items), 0)
    self.assertEqual(len(self.pool.deletes.items), 0)

  def testFlushLogLargestItems(self):
    self.pool = context._MutationPool(max_entity_count=3)
    self.pool.put(FakeEntity(tag='a'*1024*1024))
    self.assertRaises(apiproxy_errors.RequestTooLargeError, self.pool.flush)
    self.assertTrue(self.pool.puts._largest)

    self.pool = context._MutationPool(max_entity_count=3)
    self.pool.ndb_put(NdbFakeEntity(tag='a'*1024*1024))
    self.assertRaises(apiproxy_errors.RequestTooLargeError, self.pool.flush)
    self.assertTrue(self.pool.ndb_puts._largest)


class CountersTest(unittest.TestCase):
  """Test for context._Counters class."""

  def testIncrement(self):
    """Test increment() method."""
    shard_state = mock.Mock()
    counters_map = mock.Mock()
    shard_state.counters_map = counters_map
    counters = context._Counters(shard_state)

    counters_map.increment = mock.Mock()

    counters.increment('test', 19)
    counters_map.increment.assert_called_once_with('test', 19)

  def testFlush(self):
    """Test flush() method."""
    counters = context._Counters(None)
    counters.flush()


class ContextTest(testutil.HandlerTestBase):
  """Test for context.Context class."""

  def testGetSetContext(self):
    """Test module's get_context and _set functions."""
    ctx = context.Context(None, None)
    self.assertFalse(context.get())
    context.Context._set(ctx)
    self.assertEqual(ctx, context.get())
    context.Context._set(None)
    self.assertEqual(None, context.get())

  def testArbitraryPool(self):
    """Test arbitrary pool registration."""
    ctx = context.Context(None, None)
    self.assertFalse(ctx.get_pool("test"))

    pool = mock.Mock()
    ctx.register_pool("test", pool)
    self.assertEqual(pool, ctx.get_pool("test"))

    pool.flush = mock.Mock()

    ctx.flush()
    pool.flush.assert_called_once()
