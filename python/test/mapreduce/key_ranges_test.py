#!/usr/bin/env python

import unittest

from google.cloud import ndb
from mapreduce.compat import key_range

from mapreduce import key_ranges
from mapreduce import namespace_range


class FakeEntity(ndb.Model):
  foo = ndb.StringProperty(default="something")


class KeyRangesTest(unittest.TestCase):

  def testKeyRangesFromList(self):
    list_of_key_ranges = [key_range.KeyRange(ndb.Key("TestEntity", 1)),
                          key_range.KeyRange(ndb.Key("TestEntity", 2)),
                          key_range.KeyRange(ndb.Key("TestEntity", 3))]
    kranges = key_ranges.KeyRangesFactory.create_from_list(
        list(list_of_key_ranges))
    self._assertEqualsAndSerialize(list_of_key_ranges, kranges)

  def testKeyRangesFromNSRange(self):
    appid = "testbed-test"
    namespaces = ["1", "3", "5"]
    self.create_entities_in_multiple_ns(namespaces)
    ns_range = namespace_range.NamespaceRange("0", "5", _app=appid)
    kranges = key_ranges.KeyRangesFactory.create_from_ns_range(ns_range)

    expected = [key_range.KeyRange(namespace="1", _app=appid),
                key_range.KeyRange(namespace="3", _app=appid),
                key_range.KeyRange(namespace="5", _app=appid)]
    self._assertEqualsAndSerialize(expected, kranges)

  def _assertEqualsAndSerialize(self, expected, kranges):
    results = []
    while True:
      try:
        results.append(next(kranges))
        kranges = kranges.__class__.from_json(kranges.to_json())
      except StopIteration:
        break
    self.assertRaises(StopIteration, kranges.__next__)
    self.assertEqual(len(expected), len(results))

  def create_entities_in_multiple_ns(self, names):
    for name in names:
      FakeEntity(namespace=name).put()
