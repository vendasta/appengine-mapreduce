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




# Disable "Invalid method name"
# pylint: disable=g-bad-name

# os_compat must be first to ensure timezones are UTC.
# pylint: disable=g-bad-import-order
from unittest import mock
from unittest.mock import patch
from google.appengine.tools import os_compat  # pylint: disable=unused-import

import io
import datetime
import math
import os
import random
import string
import time
import unittest
import zipfile

from google.appengine.ext import ndb
from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore
from google.appengine.api import datastore_file_stub
from google.appengine.api import datastore_types
from google.appengine.api import namespace_manager
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.api.blobstore import dict_blob_storage
from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import blobstore
from google.appengine.ext import key_range
from google.appengine.ext import testbed
from google.appengine.ext.blobstore import blobstore as blobstore_internal

from mapreduce import context
from mapreduce import errors
from mapreduce import input_readers
from mapreduce import kv_pb
from mapreduce import model
from mapreduce import namespace_range
from mapreduce import records
from testlib import testutil


class AbstractDatastoreInputReaderTest(unittest.TestCase):
  """Tests for AbstractDatastoreInputReader."""

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.appid = "testbed-test"
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    namespace_manager.set_namespace(None)

  def tearDown(self):
    # Restore the scatter property setter to the original one.
    datastore_stub_util._SPECIAL_PROPERTY_MAP[
        datastore_types.SCATTER_SPECIAL_PROPERTY] = (
            False, True, datastore_stub_util._GetScatterProperty)
    self.testbed.deactivate()

  def testValidate_Passes(self):
    """Test validate function accepts valid parameters."""
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    input_readers.AbstractDatastoreInputReader.validate(mapper_spec)

  def testValidate_NoEntityFails(self):
    """Test validate function raises exception with no entity parameter."""
    params = {}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.AbstractDatastoreInputReader.validate,
                      mapper_spec)

  def testValidate_EntityKindWithNoModel(self):
    """Test validate function with bad entity kind."""
    params = {
        "entity_kind": "foo",
        }

    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    input_readers.AbstractDatastoreInputReader.validate(mapper_spec)

  def testValidate_BadBatchSize(self):
    """Test validate function rejects bad entity kind."""
    # Setting keys_only to true is an error.
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        "batch_size": "xxx"
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.AbstractDatastoreInputReader.validate,
                      mapper_spec)
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        "batch_size": "0"
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.AbstractDatastoreInputReader.validate,
                      mapper_spec)
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        "batch_size": "-1"
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.AbstractDatastoreInputReader.validate,
                      mapper_spec)

  def testValidate_WrongTypeNamespace(self):
    """Tests validate function rejects namespace of incorrect type."""
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        "namespace": 5
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.AbstractDatastoreInputReader.validate,
                      mapper_spec)

  def testChooseSplitPoints(self):
    """Tests AbstractDatastoreInputReader._choose_split_points."""
    self.assertEqual(
        [5],
        input_readers.AbstractDatastoreInputReader._choose_split_points(
            sorted([0, 9, 8, 7, 1, 2, 3, 4, 5, 6]), 2))

    self.assertEqual(
        [3, 7],
        input_readers.AbstractDatastoreInputReader._choose_split_points(
            sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 3))

    self.assertEqual(
        list(range(1, 10)),
        input_readers.AbstractDatastoreInputReader._choose_split_points(
            sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 10))

    # Too few random keys
    self.assertRaises(
        AssertionError,
        input_readers.AbstractDatastoreInputReader._choose_split_points,
        sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 11)

  def _assertEquals_splitNSByScatter(self, shards, expected, ns="",
          filters=None):
    results = input_readers.RawDatastoreInputReader._split_ns_by_scatter(
        shards, ns, "TestEntity", filters, self.appid)
    self.assertEqual(expected, results)

  def testSplitNSByScatter_NotEnoughData(self):
    """Splits should not intersect, if there's not enough data for each."""
    testutil._create_entities(list(range(2)), {"1": 1})

    expected = [key_range.KeyRange(key_start=None,
                                   key_end=testutil.key("1"),
                                   direction="ASC",
                                   include_start=False,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                key_range.KeyRange(key_start=testutil.key("1"),
                                   key_end=None,
                                   direction="ASC",
                                   include_start=True,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                None, None]
    self._assertEquals_splitNSByScatter(4, expected)

  def testSplitNSByScatter_NotEnoughData2(self):
    """Splits should not intersect, if there's not enough data for each."""
    testutil._create_entities(list(range(10)), {"2": 2, "4": 4})
    expected = [key_range.KeyRange(key_start=None,
                                   key_end=testutil.key("2"),
                                   direction="ASC",
                                   include_start=False,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                key_range.KeyRange(key_start=testutil.key("2"),
                                   key_end=testutil.key("4"),
                                   direction="ASC",
                                   include_start=True,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                key_range.KeyRange(key_start=testutil.key("4"),
                                   key_end=None,
                                   direction="ASC",
                                   include_start=True,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                None]
    self._assertEquals_splitNSByScatter(4, expected)

  def testSplitNSByScatter_LotsOfData(self):
    """Split lots of data for each shard."""
    testutil._create_entities(list(range(100)),
                              {"80": 80, "50": 50, "30": 30, "10": 10},
                              ns="google")
    expected = [key_range.KeyRange(key_start=None,
                                   key_end=testutil.key("30",
                                                        namespace="google"),
                                   direction="ASC",
                                   include_start=False,
                                   include_end=False,
                                   namespace="google",
                                   _app=self.appid),
                key_range.KeyRange(key_start=testutil.key("30",
                                                          namespace="google"),
                                   key_end=testutil.key("80",
                                                        namespace="google"),
                                   direction="ASC",
                                   include_start=True,
                                   include_end=False,
                                   namespace="google",
                                   _app=self.appid),
                key_range.KeyRange(key_start=testutil.key("80",
                                                          namespace="google"),
                                   key_end=None,
                                   direction="ASC",
                                   include_start=True,
                                   include_end=False,
                                   namespace="google",
                                   _app=self.appid),
               ]
    self._assertEquals_splitNSByScatter(3, expected, ns="google")

  def testToKeyRangesByShard(self):
    namespaces = [str(i) for i in range(3)]
    for ns in namespaces:
      testutil._create_entities(list(range(10)), {"5": 5}, ns)
    shards = 2

    expected = [
        key_range.KeyRange(key_start=None,
                           key_end=testutil.key("5", namespace="0"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="0",
                           _app=self.appid),
        key_range.KeyRange(key_start=None,
                           key_end=testutil.key("5", namespace="1"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=None,
                           key_end=testutil.key("5", namespace="2"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),

        key_range.KeyRange(key_start=testutil.key("5", namespace="0"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="0",
                           _app=self.appid),
        key_range.KeyRange(key_start=testutil.key("5", namespace="1"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=testutil.key("5", namespace="2"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),
    ]

    kranges_by_shard = (
        input_readers.AbstractDatastoreInputReader._to_key_ranges_by_shard(
            self.appid, namespaces, shards,
            model.QuerySpec(entity_kind="TestEntity")))
    self.assertEqual(shards, len(kranges_by_shard))

    expected.sort()
    results = []
    for kranges in kranges_by_shard:
      results.extend(list(kranges))
    results.sort()
    self.assertEqual(expected, results)

  def testToKeyRangesByShard_UnevenNamespaces(self):
    namespaces = [str(i) for i in range(3)]
    testutil._create_entities(list(range(10)), {"5": 5}, namespaces[0])
    testutil._create_entities(list(range(10)), {"5": 5, "6": 6}, namespaces[1])
    testutil._create_entities(list(range(10)), {"5": 5, "6": 6, "7": 7},
                              namespaces[2])
    shards = 3

    expected = [
        # shard 1
        key_range.KeyRange(key_start=None,
                           key_end=testutil.key("5", namespace="0"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="0",
                           _app=self.appid),
        key_range.KeyRange(key_start=None,
                           key_end=testutil.key("5", namespace="1"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=None,
                           key_end=testutil.key("6", namespace="2"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),
        # shard 2
        key_range.KeyRange(key_start=testutil.key("5", namespace="0"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="0",
                           _app=self.appid),
        key_range.KeyRange(key_start=testutil.key("5", namespace="1"),
                           key_end=testutil.key("6", namespace="1"),
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=testutil.key("6", namespace="2"),
                           key_end=testutil.key("7", namespace="2"),
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),
        # shard 3
        key_range.KeyRange(key_start=testutil.key("6", namespace="1"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=testutil.key("7", namespace="2"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),
    ]
    kranges_by_shard = (
        input_readers.AbstractDatastoreInputReader._to_key_ranges_by_shard(
            self.appid, namespaces, shards,
            model.QuerySpec(entity_kind="TestEntity")))
    self.assertEqual(shards, len(kranges_by_shard))

    expected.sort()
    results = []
    for kranges in kranges_by_shard:
      results.extend(list(kranges))
    results.sort()
    self.assertEqual(expected, results)


class DatastoreInputReaderTestCommon(unittest.TestCase):
  """Common tests for concrete DatastoreInputReaders."""

  # Subclass should override with its own create entities function.
  @property
  def _create_entities(self):
    return testutil._create_entities

  # Subclass should override with its own entity kind or model class path
  @property
  def entity_kind(self):
    return "TestEntity"

  # Subclass should override with its own reader class.
  @property
  def reader_cls(self):
    return input_readers.RawDatastoreInputReader

  def _get_keyname(self, entity):
    """Get keyname from an entity of certain type."""
    return entity.key().name()

  # Subclass should override with its own assert equals.
  def _assertEquals_splitInput(self, itr, keys):
    """AssertEquals helper for splitInput tests.

    Check the outputs from a single shard.

    Args:
      itr: input reader returned from splitInput.
      keys: a set of expected key names from this iterator.
    """
    results = []
    while True:
      try:
        results.append(self._get_keyname(next(iter(itr))))
        itr = itr.__class__.from_json(itr.to_json())
      except StopIteration:
        break
    results.sort()
    keys.sort()
    self.assertEqual(keys, results)

  # Subclass should override with its own assert equals.
  def _assertEqualsForAllShards_splitInput(self, keys, max_read, *itrs):
    """AssertEquals helper for splitInput tests.

    Check the outputs from all shards. This is used when sharding
    has random factor.

    Args:
      keys: a set of expected key names from this iterator.
      max_read: limit number of results read from the iterators before failing
        or None for no limit. Useful for preventing infinite loops or bounding
        the execution of the test.
      *itrs: input readers returned from splitInput.
    """
    results = []
    for itr in itrs:
      while True:
        try:
          results.append(self._get_keyname(next(iter(itr))))
          itr = itr.__class__.from_json(itr.to_json())
          if max_read is not None and len(results) > max_read:
            self.fail("Too many results found")
        except StopIteration:
          break
    results.sort()
    keys.sort()
    self.assertEqual(keys, results)

  def setUp(self):
    self.testbed = testbed.Testbed()
    unittest.TestCase.setUp(self)
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    namespace_manager.set_namespace(None)
    self._original_max = (
        self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD)
    self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD = 2

  def tearDown(self):
    # Restore the scatter property set to the original one.
    datastore_stub_util._SPECIAL_PROPERTY_MAP[
        datastore_types.SCATTER_SPECIAL_PROPERTY] = (
            False, True, datastore_stub_util._GetScatterProperty)
    # Restore max limit on ns sharding.
    self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD = (
        self._original_max)
    self.testbed.deactivate()

  def testSplitInput_withNs(self):
    self._create_entities(list(range(3)), {"1": 1}, "f")
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 2)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEqual(2, len(results))
    self._assertEqualsForAllShards_splitInput(["0", "1", "2"], None, *results)

  def testSplitInput_withNs_moreShardThanScatter(self):
    self._create_entities(list(range(3)), {"1": 1}, "f")
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 4)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertTrue(len(results) >= 2)
    self._assertEqualsForAllShards_splitInput(["0", "1", "2"], None, *results)

  def testSplitInput_noEntity(self):
    params = {
        "entity_kind": self.entity_kind,
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 1)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEqual(None, results)

  def testSplitInput_moreThanOneNS(self):
    self._create_entities(list(range(3)), {"1": 1}, "1")
    self._create_entities(list(range(10, 13)), {"11": 11}, "2")
    params = {
        "entity_kind": self.entity_kind,
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 4)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertTrue(len(results) >= 2)
    self._assertEqualsForAllShards_splitInput(
        ["0", "1", "2", "10", "11", "12"], None, *results)

  def testSplitInput_moreThanOneUnevenNS(self):
    self._create_entities(list(range(5)), {"1": 1, "3": 3}, "1")
    self._create_entities(list(range(10, 13)), {"11": 11}, "2")
    params = {
        "entity_kind": self.entity_kind,
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 4)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertTrue(len(results) >= 3)
    self._assertEqualsForAllShards_splitInput(
        ["0", "1", "2", "3", "4", "10", "11", "12"], None, *results)

  def testSplitInput_lotsOfNS(self):
    self._create_entities(list(range(3)), {"1": 1}, "9")
    self._create_entities(list(range(3, 6)), {"4": 4}, "_")
    self._create_entities(list(range(6, 9)), {"7": 7}, "a")
    params = {
        "entity_kind": self.entity_kind,
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 3)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEqual(3, len(results))
    self._assertEquals_splitInput(results[0], ["0", "1", "2"])
    self._assertEquals_splitInput(results[1], ["3", "4", "5"])
    self._assertEquals_splitInput(results[2], ["6", "7", "8"])

  def testSplitInput_withNsAndDefaultNs(self):
    shards = 2
    # 10 entities in the default namespace
    empty_ns_keys = [str(k) for k in range(10)]
    self._create_entities(empty_ns_keys,
                          {k: 1 for k in empty_ns_keys},
                          None)
    # 10 entities for each of N different non-default namespaces. The number
    # of namespaces, N, is set to be twice the cutoff for switching to sharding
    # by namespace instead of keys.
    non_empty_ns_keys = []
    for ns_num in range(self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD * 2):
      ns_keys = ["n-%02d-k-%02d" % (ns_num, k) for k in range(10)]
      non_empty_ns_keys.extend(ns_keys)
      self._create_entities(ns_keys,
                            {k: 1 for k in ns_keys},
                            "%02d" % ns_num)

    # Test a query over all namespaces
    params = {
        "entity_kind": self.entity_kind,
        "namespace": None}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params,
        shards)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEqual(shards, len(results))
    all_keys = empty_ns_keys + non_empty_ns_keys
    self._assertEqualsForAllShards_splitInput(all_keys,
                                              len(all_keys),
                                              *results)


class RawDatastoreInputReaderTest(DatastoreInputReaderTestCommon):
  """RawDatastoreInputReader specific tests."""

  @property
  def reader_cls(self):
    return input_readers.RawDatastoreInputReader

  def testValidate_Filters(self):
    """Tests validating filters parameter."""
    params = {
        "entity_kind": self.entity_kind,
        "filters": [("a", "=", 1), ("b", "=", 2)],
        }
    new = datetime.datetime.now()
    old = new.replace(year=new.year-1)
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "RawDatastoreInputReader",
        params, 1)
    self.reader_cls.validate(mapper_spec)

    # Only equality filters supported.
    params["filters"] = [["datetime_property", ">", old],
                         ["datetime_property", "<=", new],
                         ["a", "=", 1]]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

  def testEntityKindWithDot(self):
    self._create_entities(list(range(3)), {"1": 1}, "", testutil.TestEntityWithDot)

    params = {
        "entity_kind": testutil.TestEntityWithDot.kind(),
        "namespace": "",
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "RawDatastoreInputReader",
        params, 2)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEqual(2, len(results))
    self._assertEqualsForAllShards_splitInput(["0", "1", "2"], None, *results)

  def testRawEntityTypeFromOtherApp(self):
    """Test reading from other app."""
    OTHER_KIND = "bar"
    OTHER_APP = "foo"
    apiproxy_stub_map.apiproxy.GetStub("datastore_v3").SetTrusted(True)
    expected_keys = [str(i) for i in range(10)]
    for k in expected_keys:
      datastore.Put(datastore.Entity(OTHER_KIND, name=k, _app=OTHER_APP))

    params = {
        "entity_kind": OTHER_KIND,
        "_app": OTHER_APP,
    }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "RawDatastoreInputReader",
        params, 1)
    itr = self.reader_cls.split_input(mapper_spec)[0]
    self._assertEquals_splitInput(itr, expected_keys)
    apiproxy_stub_map.apiproxy.GetStub("datastore_v3").SetTrusted(False)


class DatastoreEntityInputReaderTest(RawDatastoreInputReaderTest):
  """DatastoreEntityInputReader tests."""

  @property
  def reader_cls(self):
    return input_readers.DatastoreEntityInputReader

  def _get_keyname(self, entity):
    # assert item is of low level datastore Entity type.
    self.assertTrue(isinstance(entity, datastore.Entity))
    return entity.key().name()


class DatastoreKeyInputReaderTest(RawDatastoreInputReaderTest):
  """DatastoreKeyInputReader tests."""

  @property
  def reader_cls(self):
    return input_readers.DatastoreKeyInputReader

  def _get_keyname(self, entity):
    return entity.name()


class DatastoreInputReaderTest(DatastoreInputReaderTestCommon):
  """Test DatastoreInputReader."""

  @property
  def reader_cls(self):
    return input_readers.DatastoreInputReader

  @property
  def entity_kind(self):
    return testutil.ENTITY_KIND

  def testValidate_EntityKindWithNoModel(self):
    """Test validate function with no model."""
    params = {
        "entity_kind": "foo",
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

  def testValidate_Filters(self):
    """Tests validating filters parameter."""
    params = {
        "entity_kind": self.entity_kind,
        "filters": [("a", "=", 1), ("b", "=", 2)],
        }
    new = datetime.datetime.now()
    old = new.replace(year=new.year-1)
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "DatastoreInputReader",
        params, 1)
    self.reader_cls.validate(mapper_spec)

    params["filters"] = [["a", ">", 1], ["a", "<", 2]]
    self.reader_cls.validate(mapper_spec)

    params["filters"] = [["datetime_property", ">", old],
                         ["datetime_property", "<=", new],
                         ["a", "=", 1]]
    self.reader_cls.validate(mapper_spec)

    params["filters"] = [["a", "=", 1]]
    self.reader_cls.validate(mapper_spec)

    # Invalid field c
    params["filters"] = [("c", "=", 1)]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

    # Expect a range.
    params["filters"] = [("a", "<=", 1)]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

    # Value should be a datetime.
    params["filters"] = [["datetime_property", ">", 1],
                         ["datetime_property", "<=", datetime.datetime.now()]]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

    # Expect a closed range.
    params["filters"] = [["datetime_property", ">", new],
                         ["datetime_property", "<=", old]]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

  def _set_vals(self, entities, a_vals, b_vals):
    """Set a, b values for entities."""
    vals = []
    for a in a_vals:
      for b in b_vals:
        vals.append((a, b))
    for e, val in zip(entities, vals):
      e.a = val[0]
      e.b = val[1]
      e.put()

  def testSplitInput_shardByFilters_withNs(self):
    entities = self._create_entities(list(range(12)), {}, "f")
    self._set_vals(entities, list(range(6)), list(range(2)))
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        "filters": [("a", ">", 0),
                    ("a", "<=", 3),
                    ("b", "=", 1)],
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 2)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEqual(2, len(results))
    self._assertEquals_splitInput(results[0], ["3", "5"])
    self._assertEquals_splitInput(results[1], ["7"])

  def testSplitInput_shardByFilters_noEntity(self):
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        "filters": [("a", ">", 0), ("a", "<=", 3), ("b", "=", 1)]
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 100)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEqual(3, len(results))
    self._assertEquals_splitInput(results[0], [])
    self._assertEquals_splitInput(results[1], [])
    self._assertEquals_splitInput(results[2], [])

  def testSplitInput_shardByFilters_bigShardNumber(self):
    entities = self._create_entities(list(range(12)), {}, "f")
    self._set_vals(entities, list(range(6)), list(range(2)))
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        "filters": [("a", ">", 0), ("a", "<=", 3), ("b", "=", 1)]
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 100)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEqual(3, len(results))
    self._assertEquals_splitInput(results[0], ["3"])
    self._assertEquals_splitInput(results[1], ["5"])
    self._assertEquals_splitInput(results[2], ["7"])

  def testSplitInput_shardByFilters_lotsOfNS(self):
    """Lots means more than 2 in test cases."""
    entities = self._create_entities(list(range(12)), {}, "f")
    self._set_vals(entities, list(range(6)), list(range(2)))
    entities = self._create_entities(list(range(12, 24)), {}, "g")
    self._set_vals(entities, list(range(6)), list(range(2)))
    entities = self._create_entities(list(range(24, 36)), {}, "h")
    self._set_vals(entities, list(range(6)), list(range(2)))
    entities = self._create_entities(list(range(36, 48)), {}, "h")
    self._set_vals(entities, [0]*6, list(range(2)))

    params = {
        "entity_kind": self.entity_kind,
        "filters": [("a", ">", 0), ("a", "<=", 3), ("b", "=", 1)]
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 100)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEqual(3, len(results))
    self._assertEquals_splitInput(results[0], ["3", "5", "7"])
    self._assertEquals_splitInput(results[1], ["15", "17", "19"])
    self._assertEquals_splitInput(results[2], ["27", "29", "31"])

  def testSplitInput_shardByFilters_imbalancedKeys(self):
    # Create 24 entities with keys 0..23
    scatters = {str(i): i for i in list(range(12)) + [12, 18]}
    entities = self._create_entities(list(range(24)), scatters, "f")
    # 12 of the entities have a in range(6). These all have __scatter__ set.
    # The other 12 have a = 100. Only 2 of these have __scatter__ set.
    self._set_vals(entities, list(range(6)) + [100] * 6, list(range(2)))
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        "filters": [("a", "=", 100)]
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 2)

    # Make the random.shuffle() in _to_key_ranges_by_shard() deterministic
    random.seed(2)
    results = self.reader_cls.split_input(mapper_spec)

    # Verify that the input was evenly split
    self.assertEqual(2, len(results))
    self._assertEquals_splitInput(results[0], [str(i) for i in range(12, 18)])
    self._assertEquals_splitInput(results[1], [str(i) for i in range(18, 24)])


class DatastoreInputReaderNdbTest(DatastoreInputReaderTest):

  @property
  def entity_kind(self):
    return testutil.NDB_ENTITY_KIND

  def _create_entities(self,
                       keys_itr,
                       key_to_scatter_val,
                       ns=None,
                       entity_model_cls=testutil.NdbTestEntity):
    """Create ndb entities for tests.

    Args:
      keys_itr: an iterator that contains all the key names.
        Will be casted to str.
      key_to_scatter_val: a dict that maps key names to its scatter values.
      ns: the namespace to create the entity at.
      entity_model_cls: entity model class.

    Returns:
      A list of entities created.
    """
    testutil.set_scatter_setter(key_to_scatter_val)
    entities = []
    for i in keys_itr:
      k = ndb.Key(entity_model_cls._get_kind(), str(i), namespace=ns)
      entity = entity_model_cls(key=k)
      entities.append(entity)
      entity.put()
    return entities

  def _get_keyname(self, entity):
    return entity.key.id()


BLOBSTORE_READER_NAME = (
    "mapreduce.input_readers.BlobstoreLineInputReader")


class BlobstoreLineInputReaderBlobstoreStubTest(unittest.TestCase):
  """Test the BlobstoreLineInputReader using the blobstore_stub.

  This test uses the blobstore stub to store the test data, vs the other
  test which uses mocks and the like to simulate the blobstore having data.
  """

  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testbed-test"
    from google.appengine.api.full_app_id import put
    put(self.appid)

    self.blob_storage = dict_blob_storage.DictBlobStorage()
    self.blobstore = blobstore_stub.BlobstoreServiceStub(self.blob_storage)
    # Blobstore uses datastore for BlobInfo.
    self.datastore = datastore_file_stub.DatastoreFileStub(
        self.appid, "/dev/null", "/dev/null", require_indexes=False)

    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("blobstore", self.blobstore)
    apiproxy_stub_map.apiproxy.RegisterStub("datastore_v3", self.datastore)

  def BlobInfoGet(self, blob_key):
    """Mock out BlobInfo.get instead of the datastore."""
    data = self.blob_storage.OpenBlob(blob_key)
    return MockBlobInfo(data.len)

  def CheckAllDataRead(self, data, blob_readers):
    """Check that we can read all the data with several blob readers."""
    expected_results = data.split(b"\n")
    if not expected_results[-1]:
      expected_results.pop(-1)
    actual_results = []
    for reader in blob_readers:
      while True:
        try:
          unused_offset, line = next(reader)
          actual_results.append(line)
        except StopIteration:
          break
    self.assertEqual(expected_results, actual_results)

  def EndToEndTest(self, data, shard_count):
    """Create a blobstorelineinputreader and run it through its paces."""

    blob_key = "myblob_%d" % shard_count
    self.blobstore.CreateBlob(blob_key, data)
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": [blob_key]},
        "mapper_shard_count": shard_count})
    blob_readers = input_readers.BlobstoreLineInputReader.split_input(
        mapper_spec)

    # Check that the shards cover the entire data range.
    # Another useful test would be to verify the exact splits generated or
    # the evenness of them.
    self.assertEqual(shard_count, len(blob_readers))
    previous_position = 0
    for reader in blob_readers:
      reader_info = reader.to_json()
      self.assertEqual(blob_key, reader_info["blob_key"])
      self.assertEqual(previous_position, reader_info["initial_position"])
      previous_position = reader_info["end_position"]
    self.assertEqual(len(data), previous_position)

    # See if we can read all the data with this split configuration.
    self.CheckAllDataRead(data, blob_readers)

  def TestAllSplits(self, data):
    """Test every split point by creating 2 splits, 0-m and m-n."""
    blob_key = "blob_key"
    self.blobstore.CreateBlob(blob_key, data)
    data_len = len(data)
    cls = input_readers.BlobstoreLineInputReader
    for i in range(data_len):
      chunks = []
      chunks.append(cls.from_json({
          cls.BLOB_KEY_PARAM: blob_key,
          cls.INITIAL_POSITION_PARAM: 0,
          cls.END_POSITION_PARAM: i+1}))
      chunks.append(cls.from_json({
          cls.BLOB_KEY_PARAM: blob_key,
          cls.INITIAL_POSITION_PARAM: i+1,
          cls.END_POSITION_PARAM: data_len}))
      self.CheckAllDataRead(data, chunks)

  def testEndToEnd(self):
    """End to end test of some data--split and read.."""
    # This particular pattern once caused bugs with 8 shards.
    data = b"20-questions\r\n20q\r\na\r\n"
    self.EndToEndTest(data, 8)
    self.EndToEndTest(data, 1)
    self.EndToEndTest(data, 16)
    self.EndToEndTest(data, 7)

  def testEndToEndNoData(self):
    """End to end test of some data--split and read.."""
    data = b""
    self.EndToEndTest(data, 8)

  def testEverySplit(self):
    """Test some data with every possible split point."""
    self.TestAllSplits(b"20-questions\r\n20q\r\na\r\n")
    self.TestAllSplits(b"a\nbb\nccc\ndddd\n")
    self.TestAllSplits(b"aaaa\nbbb\ncc\nd\n")

  def testEverySplitNoTrailingNewLine(self):
    """Test some data with every possible split point."""
    self.TestAllSplits(b"20-questions\r\n20q\r\na")
    self.TestAllSplits(b"a\nbb\nccc\ndddd")
    self.TestAllSplits(b"aaaa\nbbb\ncc\nd")


class MockBlobInfo:

  def __init__(self, size):
    self.size = size


class BlobstoreLineInputReaderTest(unittest.TestCase):

  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testbed-test"

  # pylint: disable=unused-argument
  def initMockedBlobstoreLineReader(self,
                                    initial_position,
                                    end_offset,
                                    buffer_size):
    input_readers.BlobstoreLineInputReader._BLOB_BUFFER_SIZE = buffer_size
    # Mock out blob key so as to avoid validation.
    blob_key_str = "foo"

    r = input_readers.BlobstoreLineInputReader(blob_key_str,
                                                  initial_position,
                                                  initial_position + end_offset)
    return r

  def assertNextEquals(self, reader, expected_k, expected_v):
    k, v = next(reader)
    self.assertEqual(expected_k, k)
    self.assertEqual(expected_v, v)

  def assertDone(self, reader):
    self.assertRaises(StopIteration, reader.__next__)

  def testAtStart(self):
    """If we start at position 0, read the first record."""
    def fetch_data(blob_key, start, end):
      return b"foo\nbar\nfoobar"[start:end + 1]
    with patch.object(blobstore_internal, "fetch_data", new=fetch_data):
      blob_reader = self.initMockedBlobstoreLineReader(
          0, 100, 100)
      self.assertNextEquals(blob_reader, 0, b"foo")
      self.assertNextEquals(blob_reader, len(b"foo\n"), b"bar")
      self.assertNextEquals(blob_reader, len(b"foo\nbar\n"), b"foobar")

  def testOmitFirst(self):
    """If we start in the middle of a record, start with the next record."""
    def fetch_data(blob_key, start, end):
      return b"foo\nbar\nfoobar"[start:end + 1]
    with patch.object(blobstore_internal, "fetch_data", new=fetch_data):
      blob_reader = self.initMockedBlobstoreLineReader(
          1, 100, 100)
      self.assertNextEquals(blob_reader, len(b"foo\n"), b"bar")
      self.assertNextEquals(blob_reader, len(b"foo\nbar\n"), b"foobar")

  def testOmitNewline(self):
    """If we start on a newline, start with the record on the next byte."""
    def fetch_data(blob_key, start, end):
      return b"foo\nbar"[start:end + 1]
    with patch.object(blobstore_internal, "fetch_data", new=fetch_data):
      blob_reader = self.initMockedBlobstoreLineReader(
          3, 100, 100)
      self.assertNextEquals(blob_reader, len(b"foo\n"), b"bar")

  def testSpanBlocks(self):
    """Test the multi block case."""
    def fetch_data(blob_key, start, end):
      return b"foo\nbar"[start:end + 1]
    with patch.object(blobstore_internal, "fetch_data", new=fetch_data):
      blob_reader = self.initMockedBlobstoreLineReader(
          0, 100, 2)
      self.assertNextEquals(blob_reader, 0, b"foo")
      self.assertNextEquals(blob_reader, len(b"foo\n"), b"bar")

  def testStopAtEnd(self):
    """If we pass end position, then we don't get a record past the end."""
    def fetch_data(blob_key, start, end):
      return b"foo\nbar"[start:end + 1]
    with patch.object(blobstore_internal, "fetch_data", new=fetch_data):
      blob_reader = self.initMockedBlobstoreLineReader(
          0, 1, 100)
      self.assertNextEquals(blob_reader, 0, b"foo")
      self.assertDone(blob_reader)

  def testDontReturnAnythingIfPassEndBeforeFirst(self):
    """Test end behavior.

    If we pass the end position when reading to the first record,
    then we don't get a record past the end.
    """
    def fetch_data(blob_key, start, end):
      return b"foo\nbar"[start:end + 1]
    with patch.object(blobstore_internal, "fetch_data", new=fetch_data):
      blob_reader = self.initMockedBlobstoreLineReader(
          3, 0, 100)
      self.assertDone(blob_reader)

  @patch.object(blobstore, 'BlobKey', new=mock.Mock)
  @patch.object(blobstore.BlobInfo, 'get', new=mock.Mock(return_value=MockBlobInfo(200)))
  def testSplitInput(self):
    mapper_spec = model.MapperSpec.from_json({
      "mapper_handler_spec": "FooHandler",
      "mapper_input_reader": BLOBSTORE_READER_NAME,
      "mapper_params": {"blob_keys": ["foo%d" % i for i in range(5)]},
      "mapper_shard_count": 1})
    blob_readers = input_readers.BlobstoreLineInputReader.split_input(
      mapper_spec)
    blob_readers_json = [r.to_json() for r in blob_readers]
    blob_readers_json.sort(key=lambda r: r["blob_key"])
    self.assertEqual([{"blob_key": "foo%d" % i,
              "initial_position": 0,
              "end_position": 200} for i in range(5)],
              blob_readers_json)

  @patch.object(blobstore, 'BlobKey', new=mock.Mock)
  @patch.object(blobstore.BlobInfo, 'get', new=mock.Mock(return_value=MockBlobInfo(200)))
  def testSplitInputMultiKey(self):
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": ["foo%d" % i for i in range(5)]},
        "mapper_shard_count": 2})
    blob_readers = input_readers.BlobstoreLineInputReader.split_input(
        mapper_spec)
    # Blob readers are built out of a dictionary of blob_keys and thus unsorted.
    blob_readers_json = [r.to_json() for r in blob_readers]
    blob_readers_json.sort(key=lambda r: r["blob_key"])
    self.assertEqual([{"blob_key": "foo%d" % i,
                        "initial_position": 0,
                        "end_position": 200} for i in range(5)],
                      blob_readers_json)

  @patch.object(blobstore, 'BlobKey', new=mock.Mock)
  @patch.object(blobstore.BlobInfo, 'get', new=mock.Mock(return_value=MockBlobInfo(199)))
  def testSplitInputMultiSplit(self):
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": ["foo"]},
        "mapper_shard_count": 2})
    blob_readers = input_readers.BlobstoreLineInputReader.split_input(
        mapper_spec)
    self.assertEqual(
        [{"blob_key": "foo",
          "initial_position": 0,
          "end_position": 99},
         {"blob_key": "foo",
          "initial_position": 99,
          "end_position": 199}],
        [r.to_json() for r in blob_readers])

  @patch.object(blobstore, 'BlobKey', new=mock.Mock)
  @patch.object(blobstore.BlobInfo, 'get', new=mock.Mock(return_value=MockBlobInfo(199)))
  def testShardDescription(self):
    """Tests the human-readable shard description."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": ["foo"]},
        "mapper_shard_count": 2})
    blob_readers = input_readers.BlobstoreLineInputReader.split_input(
        mapper_spec)
    stringified = [str(s) for s in blob_readers]
    self.assertEqual(
        ["blobstore.BlobKey('foo'):[0, 99]",
         "blobstore.BlobKey('foo'):[99, 199]"],
        stringified)

  def testTooManyKeys(self):
    """Tests when there are too many blobkeys present as input."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": ["foo"] * 1000},
        "mapper_shard_count": 2})
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.BlobstoreLineInputReader.validate,
                      mapper_spec)

  def testNoKeys(self):
    """Tests when there are no blobkeys present as input."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": []},
        "mapper_shard_count": 2})
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.BlobstoreLineInputReader.validate,
                      mapper_spec)

  @patch.object(blobstore, 'BlobKey')
  @patch.object(blobstore.BlobInfo, 'get')
  def testInvalidKey(self, mock_blob_info_get, mock_blob_key):
    """Tests when there a blobkeys in the input is invalid."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": ["foo", "nosuchblob"]},
        "mapper_shard_count": 2})
    mock_blob_key.side_effect = [blobstore.BlobKey("foo"), "nosuchblob"]
    mock_blob_info_get.side_effect = [MockBlobInfo(100), None]
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.BlobstoreLineInputReader.validate,
                      mapper_spec)


class BlobstoreZipInputReaderTest(unittest.TestCase):
  READER_NAME = (
      "mapreduce.input_readers.BlobstoreZipInputReader")

  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testbed-test"

    self.zipdata = io.BytesIO()
    archive = zipfile.ZipFile(self.zipdata, "w")
    for i in range(10):
      archive.writestr("%d.txt" % i, "%d: %s" % (i, "*"*i))
    archive.close()

  # pylint: disable=unused-argument
  def mockZipReader(self, blob_key):
    """Mocked out reader function that returns our in-memory zipfile."""
    return self.zipdata

  def testReadFirst(self):
    """Test that the first file in the zip is returned correctly."""
    reader = input_readers.BlobstoreZipInputReader("", 0, 1, self.mockZipReader)
    file_info, data_func = next(reader)
    self.assertEqual(file_info.filename, "0.txt")
    self.assertEqual(data_func(), b"0: ")

  def testReadLast(self):
    """Test we can read right up to the last file in the zip."""
    reader = input_readers.BlobstoreZipInputReader("", 9, 10,
                                                   self.mockZipReader)
    file_info, data_func = next(reader)
    self.assertEqual(file_info.filename, "9.txt")
    self.assertEqual(data_func(), b"9: *********")

  def testStopIteration(self):
    """Test that StopIteration is raised when we fetch past the end."""
    reader = input_readers.BlobstoreZipInputReader("", 0, 1, self.mockZipReader)
    next(reader)
    self.assertRaises(StopIteration, reader.__next__)

  def testSplitInput(self):
    """Test that split_input functions as expected."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.READER_NAME,
        "mapper_params": {"blob_key": ["foo"]},
        "mapper_shard_count": 2})
    readers = input_readers.BlobstoreZipInputReader.split_input(
        mapper_spec, self.mockZipReader)
    self.assertEqual(len(readers), 2)
    self.assertEqual(str(readers[0]), "blobstore.BlobKey(['foo']):[0, 7]")
    self.assertEqual(str(readers[1]), "blobstore.BlobKey(['foo']):[7, 10]")

  def testJson(self):
    """Test that we can persist/restore using the json mechanism."""
    reader = input_readers.BlobstoreZipInputReader("someblob", 0, 1,
                                                   self.mockZipReader)
    json = reader.to_json()
    self.assertEqual({"blob_key": "someblob",
                       "start_index": 0,
                       "end_index": 1},
                      json)
    reader2 = input_readers.BlobstoreZipInputReader.from_json(json)
    self.assertEqual(str(reader), str(reader2))


class BlobstoreZipLineInputReaderTest(unittest.TestCase):
  READER_NAME = ("mapreduce.input_readers."
                 "BlobstoreZipLineInputReader")

  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testbed-test"

  def create_zip_data(self, blob_count):
    """Create blob_count blobs with uneven zip data."""
    self.zipdata = {}
    blob_keys = []
    for blob_number in range(blob_count):
      stream = io.BytesIO()
      archive = zipfile.ZipFile(stream, "w")
      for file_number in range(3):
        lines = []
        for i in range(file_number + 1):
          lines.append("archive %s file %s line %s" %
                       (blob_number, file_number, i))
        archive.writestr("%d.txt" % file_number, "\n".join(lines))
      archive.close()
      blob_key = "blob%d" % blob_number
      self.zipdata[blob_key] = stream
      blob_keys.append(blob_key)

    return blob_keys

  def mockZipReader(self, blob_key):
    """Mocked out reader function that returns our in-memory zipfile."""
    return self.zipdata.get(blob_key)

  def split_input(self, blob_count, shard_count):
    """Generate some blobs and return the reader's split of them."""
    blob_keys = self.create_zip_data(blob_count)
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.READER_NAME,
        "mapper_params": {"blob_keys": blob_keys},
        "mapper_shard_count": shard_count})
    readers = input_readers.BlobstoreZipLineInputReader.split_input(
        mapper_spec, self.mockZipReader)
    return readers

  def testSplitInputOneBlob(self):
    """Simple case: split one blob into two groups."""
    readers = self.split_input(1, 2)
    self.assertEqual(2, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 2]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob0'):[2, 3]:0", str(readers[1]))

  def testSplitInputOneBlobFourShards(self):
    """Corner case: Ask for more shards than we can deliver."""
    readers = self.split_input(1, 4)
    self.assertEqual(2, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 2]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob0'):[2, 3]:0", str(readers[1]))

  def testSplitInputTwoBlobsTwoShards(self):
    """Simple case: Ask for num shards == num blobs."""
    readers = self.split_input(2, 2)
    self.assertEqual(2, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 3]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob1'):[0, 3]:0", str(readers[1]))

  def testSplitInputTwoBlobsFourShards(self):
    """Easy case: Files split nicely into blobs."""
    readers = self.split_input(2, 4)
    self.assertEqual(4, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 2]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob0'):[2, 3]:0", str(readers[1]))
    self.assertEqual("blobstore.BlobKey('blob1'):[0, 2]:0", str(readers[2]))
    self.assertEqual("blobstore.BlobKey('blob1'):[2, 3]:0", str(readers[3]))

  def testSplitInputTwoBlobsSixShards(self):
    """Corner case: Shards don't split nicely so we get too few."""
    readers = self.split_input(2, 6)
    # Note we might be able to make this return 6 with a more clever algorithm.
    self.assertEqual(4, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 2]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob0'):[2, 3]:0", str(readers[1]))
    self.assertEqual("blobstore.BlobKey('blob1'):[0, 2]:0", str(readers[2]))
    self.assertEqual("blobstore.BlobKey('blob1'):[2, 3]:0", str(readers[3]))

  def testSplitInputTwoBlobsThreeShards(self):
    """Corner case: Shards don't split nicely so we get too few."""
    readers = self.split_input(2, 3)
    # Note we might be able to make this return 3 with a more clever algorithm.
    self.assertEqual(2, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 3]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob1'):[0, 3]:0", str(readers[1]))

  def testSplitInputThreeBlobsTwoShards(self):
    """Corner case: More blobs than requested shards."""
    readers = self.split_input(3, 2)
    self.assertEqual(3, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 3]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob1'):[0, 3]:0", str(readers[1]))
    self.assertEqual("blobstore.BlobKey('blob2'):[0, 3]:0", str(readers[2]))

  def testReadOneLineFile(self):
    """Test that the first file in the zip is returned correctly."""
    self.create_zip_data(1)
    reader = input_readers.BlobstoreZipLineInputReader("blob0", 0, 1, 0,
                                                       self.mockZipReader)
    offset_info, line = next(reader)
    self.assertEqual(("blob0", 0, 0), offset_info)
    self.assertEqual(b"archive 0 file 0 line 0", line)

    # This file only has one line.
    self.assertRaises(StopIteration, reader.__next__)

  def testReadTwoLineFile(self):
    """Test that the second file in the zip is returned correctly."""
    self.create_zip_data(1)
    reader = input_readers.BlobstoreZipLineInputReader("blob0", 1, 2, 0,
                                                       self.mockZipReader)
    offset_info, line = next(reader)
    self.assertEqual(("blob0", 1, 0), offset_info)
    self.assertEqual(b"archive 0 file 1 line 0", line)

    offset_info, line = next(reader)
    self.assertEqual(("blob0", 1, 24), offset_info)
    self.assertEqual(b"archive 0 file 1 line 1", line)

    # This file only has two lines.
    self.assertRaises(StopIteration, reader.__next__)

  def testReadSecondLineFile(self):
    """Test that the second line is returned correctly."""
    self.create_zip_data(1)
    reader = input_readers.BlobstoreZipLineInputReader("blob0", 2, 3, 5,
                                                       self.mockZipReader)
    offset_info, line = next(reader)
    self.assertEqual(("blob0", 2, 24), offset_info)
    self.assertEqual(b"archive 0 file 2 line 1", line)

    # If we persist/restore the reader, the new one should pick up where
    # we left off.
    reader2 = input_readers.BlobstoreZipLineInputReader.from_json(
        reader.to_json(), self.mockZipReader)

    offset_info, line = next(reader2)
    self.assertEqual(("blob0", 2, 48), offset_info)
    self.assertEqual(b"archive 0 file 2 line 2", line)

  def testReadAcrossFiles(self):
    """Test that we can read across all the files in the single blob."""
    self.create_zip_data(1)
    reader = input_readers.BlobstoreZipLineInputReader("blob0", 0, 3, 0,
                                                       self.mockZipReader)

    for file_number in range(3):
      for i in range(file_number + 1):
        offset_info, line = next(reader)
        self.assertEqual("blob0", offset_info[0])
        self.assertEqual(file_number, offset_info[1])
        self.assertEqual(f"archive {0} file {file_number} line {i}".encode(), line)

    self.assertRaises(StopIteration, reader.__next__)

  def testJson(self):
    """Test that we can persist/restore using the json mechanism."""
    reader = input_readers.BlobstoreZipLineInputReader("blob0", 0, 3, 20,
                                                       self.mockZipReader)
    json = reader.to_json()
    self.assertEqual({"blob_key": "blob0",
                       "start_file_index": 0,
                       "end_file_index": 3,
                       "offset": 20},
                      json)
    reader2 = input_readers.BlobstoreZipLineInputReader.from_json(json)
    self.assertEqual(str(reader), str(reader2))


# Dummy start up time of a mapreduce.
STARTUP_TIME_US = 1000


class RandomStringInputReaderTest(unittest.TestCase):
  """Tests for RandomStringInputReader."""

  def testIter(self):
    input_reader = input_readers.RandomStringInputReader(10, 9)
    i = 0
    for content in input_reader:
      i += 1
      self.assertEqual(9, len(content))
    self.assertEqual(10, i)

  def testEndToEnd(self):
    mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".RandomStringInputReader",
        {
            "input_reader": {
                "count": 1000
            },
        },
        99)
    readers = input_readers.RandomStringInputReader.split_input(mapper_spec)
    i = 0
    for reader in readers:
      for _ in reader:
        i += 1
    self.assertEqual(1000, i)

  def testValidate(self):
    mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".RandomStringInputReader",
        {
            "input_reader": {
                "count": "1000"
            },
        },
        99)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.RandomStringInputReader.validate,
                      mapper_spec)

    mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".RandomStringInputReader",
        {
            "input_reader": {
                "count": -1
            },
        },
        99)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.RandomStringInputReader.validate,
                      mapper_spec)

    mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".RandomStringInputReader",
        {
            "input_reader": {
                "count": 100
            },
        },
        -1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.RandomStringInputReader.validate,
                      mapper_spec)

    mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".RandomStringInputReader",
        {
            "input_reader": {
                "count": 100,
                "string_length": 1.5
            },
        },
        99)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.RandomStringInputReader.validate,
                      mapper_spec)

  def testToFromJson(self):
    input_reader = input_readers.RandomStringInputReader(10, 9)
    reader_in_json = input_reader.to_json()
    self.assertEqual({"count": 10, "string_length": 9}, reader_in_json)
    input_readers.RandomStringInputReader.from_json(reader_in_json)
    self.assertEqual(10, input_reader._count)


class NamespaceInputReaderTest(unittest.TestCase):
  """Tests for NamespaceInputReader."""

  MAPREDUCE_READER_SPEC = ("%s.%s" %
                           (input_readers.NamespaceInputReader.__module__,
                            input_readers.NamespaceInputReader.__name__))

  def setUp(self):
    unittest.TestCase.setUp(self)
    self.app_id = "myapp"

    self.mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": NamespaceInputReaderTest.MAPREDUCE_READER_SPEC,
        "mapper_params": {"batch_size": 2},
        "mapper_shard_count": 10})


    self.datastore = datastore_file_stub.DatastoreFileStub(
        self.app_id, "/dev/null", "/dev/null")

    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("datastore_v3", self.datastore)

  def testSplitInputNoData(self):
    """Test reader with no data in datastore."""
    readers = input_readers.NamespaceInputReader.split_input(self.mapper_spec)
    self.assertEqual(10, len(readers))

    namespaces = set()
    for r in readers:
      namespaces.update(list(r))
    self.assertEqual(set(), namespaces)

  def testSplitDefaultNamespaceOnly(self):
    """Test reader with only default namespace populated."""
    testutil.TestEntity().put()
    readers = input_readers.NamespaceInputReader.split_input(self.mapper_spec)
    self.assertEqual(10, len(readers))

    namespaces = set()
    for r in readers:
      namespaces.update(list(r))
    self.assertEqual({""}, namespaces)

  def testSplitNamespacesPresent(self):
    """Test reader with multiple namespaces present."""
    testutil.TestEntity().put()
    for i in string.ascii_letters + string.digits:
      namespace_manager.set_namespace(i)
      testutil.TestEntity().put()
    namespace_manager.set_namespace(None)

    readers = input_readers.NamespaceInputReader.split_input(self.mapper_spec)
    self.assertEqual(10, len(readers))

    namespaces = set()
    for r in readers:
      namespaces.update(list(r))

    # test read
    self.assertEqual(set(list(string.ascii_letters + string.digits) + [""]),
                      namespaces)

  def testValidate_Passes(self):
    """Test validate function accepts valid parameters."""
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.NamespaceInputReader",
        {"batch_size": 10}, 1)
    input_readers.NamespaceInputReader.validate(mapper_spec)

  def testValidate_BadClassFails(self):
    """Test validate function rejects non-matching class parameter."""
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        {}, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.NamespaceInputReader.validate,
                      mapper_spec)

  def testValidate_BadBatchSize(self):
    """Test validate function rejects bad batch size."""
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.NamespaceInputReader",
        {"batch_size": "xxx"}, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.NamespaceInputReader.validate,
                      mapper_spec)

    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.NamespaceInputReader",
        {"batch_size": "0"}, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.NamespaceInputReader.validate,
                      mapper_spec)

  def testJson(self):
    """Test that we can persist/restore using the json mechanism."""
    reader = input_readers.NamespaceInputReader(
        namespace_range.NamespaceRange("", "A"))

    self.assertEqual(
        {"namespace_range": {"namespace_end": "A", "namespace_start": ""},
         "batch_size": 10},
        reader.to_json())

    testutil.TestEntity().put()
    next(iter(reader))
    json = reader.to_json()
    self.assertEqual(
        {"namespace_range": {"namespace_end": "A", "namespace_start": "-"},
         "batch_size": 10},
        json)

    self.assertEqual(
        reader.ns_range,
        input_readers.NamespaceInputReader.from_json(json).ns_range)

    self.assertEqual(
        reader._batch_size,
        input_readers.NamespaceInputReader.from_json(json)._batch_size)


def FakeCombiner(unused_key, values, left_fold):
  """Test combiner for ReducerReaderTest."""
  if left_fold:
    for value in left_fold:
      yield value
  for value in values:
    yield ord(value)


class ReducerReaderTest(testutil.CloudStorageTestBase, testutil.HandlerTestBase):
  """Tests for _ReducerReader."""

  def setUp(self):
    super().setUp()
    # Clear any context that is set.
    context.Context._set(None)

    test_filename = f"{self.gcsPrefix}/testfile"
    full_filename = f"/{self.TEST_BUCKET}/{test_filename}"

    blob = self.bucket.blob(test_filename)
    with blob.open("wb") as f:
      with records.RecordsWriter(f) as w:
        # First key is all in one record
        proto = kv_pb.KeyValues()
        proto.key = "key1"
        proto.value.extend(["a", "b"])
        w.write(proto.SerializeToString())
        # Second key is split across two records
        proto = kv_pb.KeyValues()
        proto.key = "key2"
        proto.value.extend(["c", "d"])
        w.write(proto.SerializeToString())
        proto = kv_pb.KeyValues()
        proto.key = "key2"
        proto.value.extend(["e", "f"])
        w.write(proto.SerializeToString())

    self.input_file = full_filename

  def testMultipleRequests(self):
    """Tests restoring the reader state across multiple requests."""
    # Set up a combiner for the _ReducerReader to drive.
    combiner_spec = "{}.{}".format(FakeCombiner.__module__, FakeCombiner.__name__)
    mapreduce_spec = model.MapreduceSpec(
        self.gcsPrefix,
        "DummyMapReduceJobId",
        model.MapperSpec(
            "DummyHandler",
            "DummyInputReader",
            dict(combiner_spec=combiner_spec,
                 bucket_name=self.TEST_BUCKET),
            1).to_json())
    shard_state = self.create_shard_state(0)
    ctx = context.Context(mapreduce_spec, shard_state)
    context.Context._set(ctx)

    # Now read the records in two attempts, serializing and recreating the
    # input reader as if it's a separate request. This check-points twice
    # because when we have a combiner we check-point after each record from
    # the input file is consumed and passed through the combiner function.
    reader = input_readers._ReducerReader([self.input_file])
    it = iter(reader)
    self.assertEqual(input_readers.ALLOW_CHECKPOINT, next(it))

    reader = input_readers._ReducerReader.from_json(reader.to_json())
    it = iter(reader)
    self.assertEqual(("key1", [97, 98]), next(it))

    reader = input_readers._ReducerReader.from_json(reader.to_json())
    it = iter(reader)
    self.assertEqual(input_readers.ALLOW_CHECKPOINT, next(it))

    reader = input_readers._ReducerReader.from_json(reader.to_json())
    it = iter(reader)
    self.assertEqual(("key2", [99, 100, 101, 102]), next(it))

  def testSingleRequest(self):
    """Tests when a key can be handled during a single request."""
    reader = input_readers._ReducerReader([self.input_file])
    self.assertEqual(
        [("key1", ["a", "b"]),
         input_readers.ALLOW_CHECKPOINT,
         ("key2", ["c", "d", "e", "f"])],
        list(reader))

    # now test state serialization
    reader = input_readers._ReducerReader([self.input_file])
    i = reader.__iter__()
    self.assertEqual("Ti4=", reader.to_json()["current_values"])
    self.assertEqual("Ti4=", reader.to_json()["current_key"])

    self.assertEqual(("key1", ["a", "b"]), next(i))
    self.assertEqual("KGxwMApTJ2MnCnAxCmFTJ2QnCnAyCmEu",
                      reader.to_json()["current_values"])
    self.assertEqual("UydrZXkyJwpwMAou", reader.to_json()["current_key"])

    self.assertEqual(input_readers.ALLOW_CHECKPOINT, next(i))
    self.assertEqual("KGxwMApTJ2MnCnAxCmFTJ2QnCnAyCmEu",
                      reader.to_json()["current_values"])
    self.assertEqual("UydrZXkyJwpwMAou", reader.to_json()["current_key"])

    self.assertEqual(("key2", ["c", "d", "e", "f"]), next(i))
    self.assertEqual("Ti4=", reader.to_json()["current_values"])
    self.assertEqual("Ti4=", reader.to_json()["current_key"])

    try:
      next(i)
      self.fail("Exception expected")
    except StopIteration:
      # expected
      pass

  def testSerialization(self):
    """Test deserialization at every moment."""
    reader = input_readers._ReducerReader([self.input_file])
    i = reader.__iter__()
    self.assertEqual(("key1", ["a", "b"]), next(i))

    reader = input_readers._ReducerReader.from_json(reader.to_json())
    i = reader.__iter__()
    self.assertEqual(("key2", ["c", "d", "e", "f"]), next(i))

    reader = input_readers._ReducerReader.from_json(reader.to_json())
    i = reader.__iter__()

    try:
      next(i)
      self.fail("Exception expected")
    except StopIteration:
      # expected
      pass


class GoogleCloudStorageInputTestBase(testutil.CloudStorageTestBase, testutil.HandlerTestBase):
  """Base class for running input tests with Google Cloud Storage.

  Subclasses must define READER_NAME and may redefine NUM_SHARDS.
  """

  # Defaults
  NUM_SHARDS = 10

  def create_mapper_spec(self, num_shards=None, input_params=None):
    """Create a Mapper specification using the GoogleCloudStorageInputReader.

    The specification generated uses a dummy handler and by default the
    number of shards is 10.

    Args:
      num_shards: optionally specify the number of shards.
      input_params: parameters for the input reader.

    Returns:
      a model.MapperSpec with default settings and specified input_params.
    """
    mapper_spec = model.MapperSpec(
        "DummyHandler",
        self.READER_NAME,
        {"input_reader": input_params or {}},
        num_shards or self.NUM_SHARDS)
    return mapper_spec

  def create_test_file(self, filename, content):
    """Create a test file with minimal content.

    Args:
      filename: the name of the file in the form "/bucket/object".
      content: the content to put in the file or if None a dummy string
        containing the filename will be used.
    """
    blob = self.bucket.blob(filename)
    blob.upload_from_string(content)


class GoogleCloudStorageInputReaderWithDelimiterTest(
    GoogleCloudStorageInputTestBase):
  """Tests for GoogleCloudStorageInputReader."""

  READER_CLS = input_readers.GoogleCloudStorageInputReader
  READER_NAME = input_readers.__name__ + "." + READER_CLS.__name__

  def setUp(self):
    super().setUp()

    # create some test content
    self.test_num_files = 20
    self.test_filenames = []
    for file_num in range(self.test_num_files):
      filename = f"{self.gcsPrefix}/file-{file_num:03d}"
      self.test_filenames.append(filename)
      self.create_test_file(filename, "foo")

    # Set up more directories for testing.
    self.file_per_dir = 2
    self.dirs = 20
    self.filenames_in_first_10_dirs = []
    for d in range(self.dirs):
      for file_num in range(self.file_per_dir):
        filename = f"{self.gcsPrefix}/dir-{d:02d}/file-{file_num:03d}"
        if d < 10:
          self.filenames_in_first_10_dirs.append(filename)
        self.create_test_file(filename, "foo")

  def testValidate_InvalidDelimiter(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_mapper_spec(input_params={"bucket_name": self.TEST_BUCKET,
                                              "delimiter": 1,
                                              "objects": [f"{self.gcsPrefix}/file"]}))

  def testSerialization(self):
    # Grab all files in the first 10 directories and all other files.
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/dir-0*", f"{self.gcsPrefix}/file*"],
                                              "delimiter": "/"}))
    self.assertEqual(1, len(readers))
    reader = readers[0]
    self.assertEqual(10 + self.test_num_files, len(reader._filenames))
    result_filenames = []
    while True:
      # Read one file and immediately serialize.
      reader = self.READER_CLS.from_json_str(reader.to_json_str())
      try:
        result_filenames.append(next(reader).name)
      except StopIteration:
        break
    self.assertEqual(self.file_per_dir * 10 + self.test_num_files,
                     len(result_filenames))
    self.assertEqual(self.filenames_in_first_10_dirs + self.test_filenames,
                     result_filenames)

  def testFailOnMissingInputSerialization(self):
    """Tests that pickling doesn't mess up _fail_on_missing_input."""
    # fail_on_missing_input=True.
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/dir-0*", f"{self.gcsPrefix}/file*"],
                                              "delimiter": "/",
                                              "fail_on_missing_input": True}))
    self.assertEqual(1, len(readers))
    reader = readers[0]
    self.assertTrue(reader._fail_on_missing_input)
    reader = self.READER_CLS.from_json_str(reader.to_json_str())
    self.assertTrue(reader._fail_on_missing_input)

    # fail_on_missing_input=False.
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/dir-0*", f"{self.gcsPrefix}/file*"],
                                              "delimiter": "/",
                                              "fail_on_missing_input": False}))
    self.assertEqual(1, len(readers))
    reader = readers[0]
    self.assertFalse(reader._fail_on_missing_input)
    reader = self.READER_CLS.from_json_str(reader.to_json_str())
    self.assertFalse(reader._fail_on_missing_input)

    # fail_on_missing_input not present in json.
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/dir-0*", f"{self.gcsPrefix}/file*"],
                                              "delimiter": "/"}))
    self.assertEqual(1, len(readers))
    reader = readers[0]
    self.assertFalse(reader._fail_on_missing_input)
    json_dict = reader.to_json()
    self.assertTrue("fail_on_missing_input" in json_dict)
    del json_dict["fail_on_missing_input"]
    reader = self.READER_CLS.from_json(json_dict)
    # _fail_on_missing_input defaults to False.
    self.assertFalse(reader._fail_on_missing_input)


  def testSplit(self):
    # Grab all files in the first 10 directories.
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/dir-0*"],
                                              "delimiter": "/"}))
    self.assertEqual(1, len(readers))
    reader = readers[0]
    self.assertEqual(10, len(reader._filenames))
    result_filenames = [f._blob.name for f in reader]
    self.assertEqual(self.file_per_dir * 10, len(result_filenames))
    self.assertEqual(self.filenames_in_first_10_dirs, result_filenames)

    # Grab all files.
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/*"],
                                              "delimiter": "/"}))
    self.assertEqual(1, len(readers))
    reader = readers[0]
    self.assertEqual(self.dirs + self.test_num_files, len(reader._filenames))
    self.assertEqual(self.file_per_dir * self.dirs + self.test_num_files,
                     len([f for f in reader]))


class GoogleCloudStorageInputReaderTest(GoogleCloudStorageInputTestBase):
  """Tests for GoogleCloudStorageInputReader."""

  READER_CLS = input_readers.GoogleCloudStorageInputReader
  READER_NAME = input_readers.__name__ + "." + READER_CLS.__name__

  def setUp(self):
    super().setUp()

    # create test content
    self.test_content = []
    self.test_num_files = 20
    self.test_filenames = []
    for file_num in range(self.test_num_files):
      content = "Dummy Content %03d" % file_num
      self.test_content.append(content)
      filename = f"{self.gcsPrefix}/file-{file_num:03d}"
      self.test_filenames.append(filename)
      self.create_test_file(filename, content)

  def testValidate_NoParams(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_mapper_spec())

  def testValidate_NoBucket(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_mapper_spec(input_params={"objects": ["1", "2", "3"]}))

  def testValidate_NoObjects(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_mapper_spec(input_params={"bucket_name": self.TEST_BUCKET}))

  def testValidate_NonList(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_mapper_spec(input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": "1"}))

  def testValidate_SingleObject(self):
    # expect no errors are raised
    self.READER_CLS.validate(
        self.create_mapper_spec(input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": ["1"]}))

  def testValidate_PassesWithBucketFromMapperSpec(self):
    # expect no errors are raised
    self.READER_CLS.validate(
        model.MapperSpec(
            "DummyHandler",
            self.READER_NAME,
            {"bucket_name": self.TEST_BUCKET,
             "input_reader": {"objects": ["1", "2", "3"]}},
            self.NUM_SHARDS))

  def testGetParams_DoesntOverwriteInputReaderBucket(self):
    # expect no errors are raised
    params = self.READER_CLS.get_params(
        model.MapperSpec(
            "DummyHandler",
            self.READER_NAME,
            {"bucket_name": "bad",
             "input_reader": {"bucket_name": "good",
                              "objects": ["1", "2", "3"]}},
            self.NUM_SHARDS))
    self.assertEqual(params["bucket_name"], "good")

  def testValidate_ObjectList(self):
    # expect no errors are raised
    self.READER_CLS.validate(
        self.create_mapper_spec(input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": ["1", "2", "3"]}))

  def testValidate_ObjectListNonString(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_mapper_spec(input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": ["1", ["2", "3"]]}))

  def testSplit_NoObjectSingleShard(self):
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": []}))
    self.assertFalse(readers)

  def testSplit_SingleObjectSingleShard(self):
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": ["1"]}))
    self.assertEqual(1, len(readers))
    self.assertEqual(1, len(readers[0]._filenames))

  def testSplit_SingleObjectManyShards(self):
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=10,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": ["1"]}))
    self.assertEqual(1, len(readers))
    self.assertEqual(1, len(readers[0]._filenames))

  def testSplit_ManyObjectEvenlySplitManyShards(self):
    num_shards = 10
    files_per_shard = 3
    filenames = ["f-%d" % f for f in range(num_shards * files_per_shard)]
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=num_shards,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": filenames}))
    self.assertEqual(num_shards, len(readers))
    for reader in readers:
      self.assertEqual(files_per_shard, len(reader._filenames))

  def testSplit_ManyObjectUnevenlySplitManyShards(self):
    num_shards = 10
    total_files = int(10 * 2.33)
    filenames = ["f-%d" % f for f in range(total_files)]
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=num_shards,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": filenames}))
    self.assertEqual(num_shards, len(readers))
    found_files = 0
    for reader in readers:
      shard_num_files = len(reader._filenames)
      found_files += shard_num_files
      # ensure per-shard distribution is even
      min_files = math.floor(total_files / num_shards)
      max_files = min_files + 1
      self.assertTrue(min_files <= shard_num_files,
                      msg="Too few files (%d > %d) in reader: %s" %
                      (min_files, shard_num_files, reader))
      self.assertTrue(max_files >= shard_num_files,
                      msg="Too many files (%d < %d) in reader: %s" %
                      (max_files, shard_num_files, reader))
    self.assertEqual(total_files, found_files)

  def testSplit_Wildcard(self):
    # test prefix matching all files
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/file-*"]}))
    self.assertEqual(1, len(readers))
    self.assertEqual(self.test_num_files, len(readers[0]._filenames))

    # test prefix the first 10 (those with 00 prefix)
    self.assertTrue(self.test_num_files > 10,
                    msg="More than 10 files required for testing")
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/file-00*"]}))
    self.assertEqual(1, len(readers))
    self.assertEqual(10, len(readers[0]._filenames))

    # test prefix matching no files
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/badprefix*"]}))
    self.assertEqual(0, len(readers))

  def testNext(self):
    mapper_spec = self.create_mapper_spec(num_shards=1,
                                          input_params={"bucket_name":
                                                        self.TEST_BUCKET,
                                                        "objects": [f"{self.gcsPrefix}/file-*"]})
    mapreduce_spec = model.MapreduceSpec(
        self.gcsPrefix,
        "DummyMapReduceJobId",
        mapper_spec.to_json())
    shard_state = self.create_shard_state(0)
    ctx = context.Context(mapreduce_spec, shard_state)
    context.Context._set(ctx)

    # Counter should be 0.
    self.assertEqual(0, shard_state.counters_map.get(
        input_readers.COUNTER_IO_READ_MSEC))
    readers = self.READER_CLS.split_input(mapper_spec)
    self.assertEqual(1, len(readers))
    reader_files = list(readers[0])
    self.assertEqual(self.test_num_files, len(reader_files))
    found_content = []
    for reader_file in reader_files:
      found_content.append(reader_file.read())
    self.assertEqual(len(self.test_content), len(found_content))
    for content in self.test_content:
      self.assertTrue(content.encode() in found_content)
    # Check that the counter was incremented.
    self.assertTrue(shard_state.counters_map.get(
        input_readers.COUNTER_IO_READ_MSEC) > 0)

  def testNextWithMissingFiles(self):
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/file-*"]}))
    self.assertEqual(1, len(readers))

    # Remove the first and second to last files.
    self.bucket.blob(self.test_filenames[0].removeprefix(f'/{self.TEST_BUCKET}/')).delete()
    self.bucket.blob(self.test_filenames[-2].removeprefix(f'/{self.TEST_BUCKET}/')).delete()
    del self.test_filenames[0]
    del self.test_filenames[-2]

    reader_files = list(readers[0])
    self.assertEqual(len(self.test_filenames), len(reader_files))
    self.assertEqual(self.test_filenames, [f._blob.name for f in reader_files])

  def testSerialization(self):
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/file-*"]}))
    self.assertEqual(1, len(readers))
    # serialize/deserialize unused reader
    reader = self.READER_CLS.from_json(readers[0].to_json())

    found_content = []
    for _ in range(self.test_num_files):
      reader_file = next(reader)
      found_content.append(reader_file.read())
      # serialize/deserialize after each file is read
      reader = self.READER_CLS.from_json(reader.to_json())
    self.assertEqual(len(self.test_content), len(found_content))
    for content in self.test_content:
      self.assertTrue(content.encode() in found_content)

    # verify a reader at EOF still raises EOF after serialization
    self.assertRaises(StopIteration, reader.__next__)


class GoogleCloudStorageRecordInputReaderTest(GoogleCloudStorageInputTestBase):
  """Tests for GoogleCloudStorageInputReader."""

  READER_CLS = input_readers.GoogleCloudStorageRecordInputReader
  READER_NAME = input_readers.__name__ + "." + READER_CLS.__name__

  def create_test_file(self, filename, content):
    """Create a test LevelDB file with a RecordWriter.

    Args:
      filename: the name of the file in the form "/bucket/object".
      content: list of content to put in file in LevelDB format.
    """
    blob = self.bucket.blob(filename)
    test_file = blob.open("wb")
    with records.RecordsWriter(test_file) as w:
      for c in content:
        w.write(c)
    test_file.close()

  def testSingleFileNoRecord(self):
    filename = f"{self.gcsPrefix}/empty-file"
    self.create_test_file(filename, [])
    reader = self.READER_CLS([filename])

    with self.assertRaises(StopIteration):
      next(reader)

  def testSingleFileOneRecord(self):
    filename = f"{self.gcsPrefix}/single-record-file"
    data = b"foobardata"
    self.create_test_file(filename, [data])
    reader = self.READER_CLS([filename])

    self.assertEqual(data, next(reader))
    self.assertRaises(StopIteration, reader.__next__)

  def testSingleFileManyRecords(self):
    filename = f"{self.gcsPrefix}/many-records-file"
    data = []
    for record_num in range(100):  # Make 100 records
      data.append((b"%03d" % record_num) * 10)  # Make each record 30 chars long
    self.create_test_file(filename, data)
    reader = self.READER_CLS([filename])

    for record in data:
      self.assertEqual(record, next(reader))
    self.assertRaises(StopIteration, reader.__next__)
    # ensure StopIteration is still raised after its first encountered
    self.assertRaises(StopIteration, reader.__next__)

  def testSingleFileManyKeyValuesRecords(self):
    filename = f"{self.gcsPrefix}/many-key-values-records-file"
    input_data = [(str(i), ["_" + str(i), "_" + str(i)]) for i in range(100)]

    blob = self.bucket.blob(filename)
    with blob.open("wb") as f:
      with records.RecordsWriter(f) as w:
        for (k, v) in input_data:
          proto = kv_pb.KeyValues()
          proto.key = k
          proto.value.extend(v)
          b = proto.SerializeToString()
          w.write(b)
    reader = self.READER_CLS([f"/{self.TEST_BUCKET}/{filename}"])

    output_data = []
    for record in reader.__iter__():
      proto = kv_pb.KeyValues()
      proto.ParseFromString(record)
      output_data.append((proto.key, proto.value))

    self.assertEqual(input_data, output_data)
    self.assertRaises(StopIteration, reader.__next__)
    # ensure StopIteration is still raised after its first encountered
    self.assertRaises(StopIteration, reader.__next__)

  def testManyFilesManyRecords(self):
    filenames = []
    all_data = []
    for file_num in range(10):  # Make 10 files
      filename = f"{self.gcsPrefix}/file-{file_num:03d}"
      data_set = []
      for record_num in range(10):  # Make 10 records, each 30 chars long
        data_set.append((b"%03d" % record_num) * 10)
      self.create_test_file(filename, data_set)
      filenames.append(filename)
      all_data.append(data_set)
    reader = self.READER_CLS(filenames)

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, next(reader))
    self.assertRaises(StopIteration, reader.__next__)

  def testManyFilesSomeEmpty(self):
    filenames = []
    all_data = []
    for file_num in range(50):  # Make 10 files
      filename = f"{self.gcsPrefix}/file-{file_num:03d}"
      data_set = []
      for record_num in range(file_num % 5):  # Make up to 4 records
        data_set.append((b"%03d" % record_num) * 10)
      self.create_test_file(filename, data_set)
      filenames.append(filename)
      all_data.append(data_set)
    reader = self.READER_CLS(filenames)

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, next(reader))
    self.assertRaises(StopIteration, reader.__next__)

  def testSerialization(self):
    filenames = []
    all_data = []
    for file_num in range(50):  # Make 10 files
      filename = f"{self.gcsPrefix}/file-{file_num:03d}"
      data_set = []
      for record_num in range(file_num % 5):  # Make up to 4 records
        data_set.append((b"%03d" % record_num) * 10)
      self.create_test_file(filename, data_set)
      filenames.append(filename)
      all_data.append(data_set)
    reader = self.READER_CLS(filenames)

    # Serialize before using
    reader = self.READER_CLS.from_json(reader.to_json())

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, next(reader))
        # Serialize after each read
        reader = self.READER_CLS.from_json(reader.to_json())
    self.assertRaises(StopIteration, reader.__next__)

    # Serialize after StopIteration reached
    reader = self.READER_CLS.from_json(reader.to_json())
    self.assertRaises(StopIteration, reader.__next__)

  def testCounters(self):
    filenames = []
    all_data = []
    for file_num in range(10):  # Make 10 files
      filename = f"{self.gcsPrefix}/file-{file_num:03d}"
      data_set = []
      for record_num in range(10):  # Make 10 records, each 30 chars long
        data_set.append((b"%03d" % record_num) * 10)
      self.create_test_file(filename, data_set)
      filenames.append(filename)
      all_data.append(data_set)

    mapper_spec = self.create_mapper_spec(num_shards=1,
                                          input_params={"bucket_name":
                                                        self.TEST_BUCKET,
                                                        "objects": [filenames]})
    mapreduce_spec = model.MapreduceSpec(
        "DummyMapReduceJobName",
        "DummyMapReduceJobId",
        mapper_spec.to_json())
    shard_state = self.create_shard_state(0)
    ctx = context.Context(mapreduce_spec, shard_state)
    context.Context._set(ctx)

    # Counters should be 0.
    self.assertEqual(0, shard_state.counters_map.get(
        input_readers.COUNTER_IO_READ_MSEC))
    self.assertEqual(0, shard_state.counters_map.get(
        input_readers.COUNTER_IO_READ_BYTES))

    reader = self.READER_CLS(filenames)

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, next(reader))
    self.assertRaises(StopIteration, reader.__next__)

    # Check counters.
    self.assertTrue(shard_state.counters_map.get(
        input_readers.COUNTER_IO_READ_MSEC) > 0)
    # Check for 10 files each with 10 records each 30 chars long.
    self.assertEqual(10 * 10 * 30, shard_state.counters_map.get(
        input_readers.COUNTER_IO_READ_BYTES))


