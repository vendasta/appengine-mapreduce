#!/usr/bin/env python


# pylint: disable=g-bad-name

import os
import unittest
from google.cloud import ndb

from mapreduce.compat import key_range

from mapreduce import datastore_range_iterators as db_iters
from mapreduce import key_ranges
from mapreduce import model
from mapreduce import namespace_range
from mapreduce import property_range
from mapreduce import util


class FakeEntity(ndb.Model):
  bar = ndb.IntegerProperty()


class NdbFakeEntity(ndb.Model):
  bar = ndb.IntegerProperty()


class IteratorTest(unittest.TestCase):

  def setUp(self):
    self.app = "foo"
    self.namespaces = [str(i) for i in range(5)]
    self.filters = [("bar", "=", 0)]
    self.key_names = [str(i) for i in range(10)]
    self.bar_values = [0]*5 + list(range(1, 6))

  def _create_entities(self):
    entities = []
    for namespace in self.namespaces:
      for key_name, bar in zip(self.key_names, self.bar_values):
        key = ndb.Key("FakeEntity", key_name, namespace=namespace)
        entity = FakeEntity(key=key, bar=bar)
        entities.append(entity)
        entity.put()
    return entities

  def _create_ndb_entities(self):
    entities = []
    for namespace in self.namespaces:
      for key_name, bar in zip(self.key_names, self.bar_values):
        key = ndb.Key("NdbFakeEntity", key_name, namespace=namespace)
        entity = NdbFakeEntity(key=key, bar=bar)
        entities.append(entity)
        entity.put()
    return entities

  def _serializeAndAssertEquals(self, itr, expected, get_key):
    itr = itr.__class__.from_json_str(itr.to_json_str())
    results = []
    for _ in expected:
      key = get_key(next(iter(itr)))
      results.append(key)
      itr = itr.__class__.from_json_str(itr.to_json_str())
      itr = itr.__class__.from_json_str(itr.to_json_str())
    results.sort()
    self.assertEqual(expected, results)
    self.assertRaises(StopIteration, iter(itr).__next__)

  def _AssertEquals(self, itr, expected, get_key):
    results = [get_key(i) for i in itr]
    results.sort()
    self.assertEqual(expected, results)


class PropertyRangeIteratorTest(IteratorTest):

  def setUp(self):
    super().setUp()
    self.filters = [("bar", "<=", 4), ("bar", ">", 2)]

  def _create_iter(self, entity_kind):
    query_spec = model.QuerySpec(entity_kind=util.get_short_name(entity_kind),
                                 batch_size=10,
                                 filters=self.filters,
                                 model_class_path=entity_kind)
    p_range = property_range.PropertyRange(self.filters,
                                           entity_kind)
    ns_range = namespace_range.NamespaceRange(self.namespaces[0],
                                              self.namespaces[-1])
    itr = db_iters.RangeIteratorFactory.create_property_range_iterator(
        p_range, ns_range, query_spec)
    return itr

  def testE2e(self):
    entities = self._create_entities()
    expected = [e.key for e in entities if e.bar in [3, 4]]
    expected.sort()

    itr = self._create_iter(f"{FakeEntity.__module__}.FakeEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key)
    itr = self._create_iter(f"{FakeEntity.__module__}.FakeEntity")
    self._AssertEquals(itr, expected, lambda e: e.key)

  def testE2eNdb(self):
    entities = self._create_ndb_entities()
    expected = [e.key for e in entities if e.bar in [3, 4]]
    expected.sort()

    itr = self._create_iter(f"{NdbFakeEntity.__module__}.NdbFakeEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key)
    itr = self._create_iter(f"{NdbFakeEntity.__module__}.NdbFakeEntity")
    self._AssertEquals(itr, expected, lambda e: e.key)


class KeyRangesIteratorTest(IteratorTest):

  def _create_iter(self, iter_cls, entity_kind):
    kranges = [key_range.KeyRange(namespace=ns) for ns in self.namespaces]
    kranges = key_ranges.KeyRangesFactory.create_from_list(kranges)
    query_spec = model.QuerySpec(entity_kind=util.get_short_name(entity_kind),
                                 batch_size=10,
                                 filters=self.filters,
                                 model_class_path=entity_kind)
    itr = db_iters.RangeIteratorFactory.create_key_ranges_iterator(
        kranges, query_spec, iter_cls)
    return itr

  def testE2e(self):
    entities = self._create_entities()
    expected = [e.key for e in entities if e.bar == 0]
    expected.sort()

    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            f"{FakeEntity.__module__}.FakeEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key)
    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            f"{FakeEntity.__module__}.FakeEntity")
    self._AssertEquals(itr, expected, lambda e: e.key)

  def testE2eNdb(self):
    entities = self._create_ndb_entities()
    expected = [e.key for e in entities if e.bar == 0]
    expected.sort()

    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            f"{NdbFakeEntity.__module__}.NdbFakeEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key)

    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            f"{NdbFakeEntity.__module__}.NdbFakeEntity")
    self._AssertEquals(itr, expected, lambda e: e.key)


class KeyRangeIteratorTestBase(IteratorTest):

  def setUp(self):
    super().setUp()
    self.namespace = "foo_ns"
    self.namespaces = [self.namespace]

  def _create_iter(self, iter_cls, entity_kind):
    key_start = ndb.Key(util.get_short_name(entity_kind),
                        "0",
                        namespace=self.namespace)
    key_end = ndb.Key(util.get_short_name(entity_kind),
                      "999",
                      namespace=self.namespace)
    krange = key_range.KeyRange(key_start,
                                key_end,
                                include_start=True,
                                include_end=True,
                                namespace=self.namespace)

    query_spec = model.QuerySpec(entity_kind=util.get_short_name(entity_kind),
                                 batch_size=10,
                                 filters=self.filters,
                                 model_class_path=entity_kind)
    return iter_cls(krange, query_spec)


class KeyRangeModelIteratorTest(KeyRangeIteratorTestBase):

  def testE2e(self):
    entities = self._create_entities()
    expected = [e.key for e in entities if e.bar == 0]
    expected.sort()

    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            f"{FakeEntity.__module__}.FakeEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key)
    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            f"{FakeEntity.__module__}.FakeEntity")
    self._AssertEquals(itr, expected, lambda e: e.key)

  def testE2eNdb(self):
    entities = self._create_ndb_entities()
    expected = [e.key for e in entities if e.bar == 0]
    expected.sort()

    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            f"{NdbFakeEntity.__module__}.NdbFakeEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key)
    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            f"{NdbFakeEntity.__module__}.NdbFakeEntity")
    self._AssertEquals(itr, expected, lambda e: e.key)


class KeyRangeRawEntityIteratorTest(KeyRangeIteratorTestBase):

  def testE2e(self):
    entities = self._create_entities()
    expected = [e.key for e in entities if e.bar == 0]
    expected.sort()

    itr = self._create_iter(db_iters.KeyRangeEntityIterator, "FakeEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key)
    itr = self._create_iter(db_iters.KeyRangeEntityIterator, "FakeEntity")
    self._AssertEquals(itr, expected, lambda e: e.key)

    itr = self._create_iter(db_iters.KeyRangeKeyIterator, "FakeEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e)
    itr = self._create_iter(db_iters.KeyRangeKeyIterator, "FakeEntity")
    self._AssertEquals(itr, expected, lambda e: e)


class KeyRangeEntityProtoIteratorTest(KeyRangeIteratorTestBase):

  def testE2e(self):
    entities = self._create_entities()
    expected = [e.key for e in entities if e.bar == 0]
    expected.sort()

    itr = self._create_iter(db_iters.KeyRangeEntityProtoIterator, "FakeEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key)
    itr = self._create_iter(db_iters.KeyRangeEntityProtoIterator, "FakeEntity")
    self._AssertEquals(itr, expected, lambda e: e.key)
