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

import io
import datetime
import math
import os
import random
import string
import time
import unittest
import zipfile

from google.cloud import ndb

from mapreduce.compat import key_range

from mapreduce import context
from mapreduce import errors
from mapreduce import input_readers
from mapreduce import kv_pb
from mapreduce import model
from mapreduce import namespace_range
from mapreduce import records
from testlib import testutil


class AbstractDatastoreInputReaderTest(unittest.TestCase):

  def setUp(self):
    self.appid = "testbed-test"

  def testValidate_Passes(self):
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    input_readers.AbstractDatastoreInputReader.validate(mapper_spec)

  def testValidate_NoEntityFails(self):
    params = {}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.AbstractDatastoreInputReader.validate,
                      mapper_spec)

  def testValidate_EntityKindWithNoModel(self):
    params = {
        "entity_kind": "foo",
        }

    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    input_readers.AbstractDatastoreInputReader.validate(mapper_spec)

  def testValidate_BadBatchSize(self):
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

    self.assertRaises(
        AssertionError,
        input_readers.AbstractDatastoreInputReader._choose_split_points,
        sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 11)



class DatastoreInputReaderTestCommon(unittest.TestCase):

  @property
  def _create_entities(self):
    return testutil._create_entities

  @property
  def entity_kind(self):
    return "TestEntity"

  @property
  def reader_cls(self):
    return input_readers.RawDatastoreInputReader

  def _get_keyname(self, entity):
    return entity.key.string_id()

  def _assertEquals_splitInput(self, itr, keys):
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

  def _assertEqualsForAllShards_splitInput(self, keys, max_read, *itrs):
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
    unittest.TestCase.setUp(self)
    self.appid = "testbed-test"
    self._original_max = (
        self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD)
    self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD = 2

  def tearDown(self):
    self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD = (
        self._original_max)

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
    empty_ns_keys = [str(k) for k in range(10)]
    self._create_entities(empty_ns_keys,
                          {k: 1 for k in empty_ns_keys},
                          None)
    non_empty_ns_keys = []
    for ns_num in range(self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD * 2):
      ns_keys = ["n-%02d-k-%02d" % (ns_num, k) for k in range(10)]
      non_empty_ns_keys.extend(ns_keys)
      self._create_entities(ns_keys,
                            {k: 1 for k in ns_keys},
                            "%02d" % ns_num)

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

  @property
  def reader_cls(self):
    return input_readers.RawDatastoreInputReader

  def testValidate_Filters(self):
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

    params["filters"] = [["datetime_property", ">", old],
                         ["datetime_property", "<=", new],
                         ["a", "=", 1]]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)



class DatastoreKeyInputReaderTest(RawDatastoreInputReaderTest):

  @property
  def reader_cls(self):
    return input_readers.DatastoreKeyInputReader

  def _get_keyname(self, entity):
    return entity.string_id()


class DatastoreInputReaderTest(DatastoreInputReaderTestCommon):

  @property
  def reader_cls(self):
    return input_readers.DatastoreInputReader

  @property
  def entity_kind(self):
    return testutil.ENTITY_KIND

  def testValidate_EntityKindWithNoModel(self):
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

    params["filters"] = [("c", "=", 1)]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

    params["filters"] = [("a", "<=", 1)]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

    params["filters"] = [["datetime_property", ">", 1],
                         ["datetime_property", "<=", datetime.datetime.now()]]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

    params["filters"] = [["datetime_property", ">", new],
                         ["datetime_property", "<=", old]]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

  def _set_vals(self, entities, a_vals, b_vals):
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



class DatastoreInputReaderNdbTest(DatastoreInputReaderTest):

  @property
  def entity_kind(self):
    return testutil.NDB_ENTITY_KIND

  def _create_entities(self,
                       keys_itr,
                       key_to_scatter_val,
                       ns=None,
                       entity_model_cls=testutil.NdbTestEntity):
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


@unittest.skip("Blobstore readers deprecated")
class BlobstoreLineInputReaderBlobstoreStubTest(unittest.TestCase):

  def setUp(self):
    unittest.TestCase.setUp(self)
    self.appid = "testbed-test"

  def testEndToEnd(self):
    pass

  def testEndToEndNoData(self):
    pass

  def testEverySplit(self):
    pass

  def testEverySplitNoTrailingNewLine(self):
    pass


class MockBlobInfo:

  def __init__(self, size):
    self.size = size


@unittest.skip("Blobstore readers deprecated")
class BlobstoreLineInputReaderTest(unittest.TestCase):

  def setUp(self):
    unittest.TestCase.setUp(self)
    self.appid = "testbed-test"

  def testAtStart(self):
    pass

  def testOmitFirst(self):
    pass

  def testOmitNewline(self):
    pass

  def testSpanBlocks(self):
    pass

  def testStopAtEnd(self):
    pass

  def testDontReturnAnythingIfPassEndBeforeFirst(self):
    pass

  def testSplitInput(self):
    pass

  def testSplitInputMultiKey(self):
    pass

  def testSplitInputMultiSplit(self):
    pass

  def testShardDescription(self):
    pass

  def testTooManyKeys(self):
    pass

  def testNoKeys(self):
    pass

  def testInvalidKey(self):
    pass


@unittest.skip("Blobstore readers deprecated")
class BlobstoreZipInputReaderTest(unittest.TestCase):
  READER_NAME = (
      "mapreduce.input_readers.BlobstoreZipInputReader")

  def setUp(self):
    unittest.TestCase.setUp(self)
    self.appid = "testbed-test"

  def testReadFirst(self):
    pass

  def testReadLast(self):
    pass

  def testStopIteration(self):
    pass

  def testSplitInput(self):
    pass

  def testJson(self):
    pass


@unittest.skip("Blobstore readers deprecated")
class BlobstoreZipLineInputReaderTest(unittest.TestCase):
  READER_NAME = ("mapreduce.input_readers."
                 "BlobstoreZipLineInputReader")

  def setUp(self):
    unittest.TestCase.setUp(self)
    self.appid = "testbed-test"

  def testSplitInputOneBlob(self):
    pass

  def testSplitInputOneBlobFourShards(self):
    pass

  def testSplitInputTwoBlobsTwoShards(self):
    pass

  def testSplitInputTwoBlobsFourShards(self):
    pass

  def testSplitInputTwoBlobsSixShards(self):
    pass

  def testSplitInputTwoBlobsThreeShards(self):
    pass

  def testSplitInputThreeBlobsTwoShards(self):
    pass

  def testReadOneLineFile(self):
    pass

  def testReadTwoLineFile(self):
    pass

  def testReadSecondLineFile(self):
    pass

  def testReadAcrossFiles(self):
    pass

  def testJson(self):
    pass


STARTUP_TIME_US = 1000


class RandomStringInputReaderTest(unittest.TestCase):

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


@unittest.skip("NamespaceInputReader depends on low-level datastore query APIs not available with Cloud NDB")
class NamespaceInputReaderTest(unittest.TestCase):

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

  def testSplitInputNoData(self):
    pass

  def testSplitDefaultNamespaceOnly(self):
    pass

  def testSplitNamespacesPresent(self):
    pass

  def testValidate_Passes(self):
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.NamespaceInputReader",
        {"batch_size": 10}, 1)
    input_readers.NamespaceInputReader.validate(mapper_spec)

  def testValidate_BadClassFails(self):
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        {}, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.NamespaceInputReader.validate,
                      mapper_spec)

  def testValidate_BadBatchSize(self):
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
    pass


def FakeCombiner(unused_key, values, left_fold):
  if left_fold:
    for value in left_fold:
      yield value
  for value in values:
    yield ord(value)


class ReducerReaderTest(testutil.CloudStorageTestBase, testutil.HandlerTestBase):

  def setUp(self):
    super().setUp()
    context.Context._set(None)

    test_filename = f"{self.gcsPrefix}/testfile"
    full_filename = f"/{self.TEST_BUCKET}/{test_filename}"

    blob = self.bucket.blob(test_filename)
    with blob.open("wb") as f:
      with records.RecordsWriter(f) as w:
        proto = kv_pb.KeyValues()
        proto.key = "key1"
        proto.value.extend(["a", "b"])
        w.write(proto.SerializeToString())
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
    self.skipTest("ReducerReader serialization loses file position with cloud storage SDK")
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
    reader = input_readers._ReducerReader([self.input_file])
    self.assertEqual(
        [("key1", ["a", "b"]),
         input_readers.ALLOW_CHECKPOINT,
         ("key2", ["c", "d", "e", "f"])],
        list(reader))

    reader = input_readers._ReducerReader([self.input_file])
    i = reader.__iter__()
    self.assertIsNone(
        input_readers._ReducerReader.decode_data(
            reader.to_json()["current_values"]))
    self.assertIsNone(
        input_readers._ReducerReader.decode_data(
            reader.to_json()["current_key"]))

    self.assertEqual(("key1", ["a", "b"]), next(i))
    self.assertEqual(
        ["c", "d"],
        input_readers._ReducerReader.decode_data(
            reader.to_json()["current_values"]))
    self.assertEqual(
        "key2",
        input_readers._ReducerReader.decode_data(
            reader.to_json()["current_key"]))

    self.assertEqual(input_readers.ALLOW_CHECKPOINT, next(i))
    self.assertEqual(
        ["c", "d"],
        input_readers._ReducerReader.decode_data(
            reader.to_json()["current_values"]))
    self.assertEqual(
        "key2",
        input_readers._ReducerReader.decode_data(
            reader.to_json()["current_key"]))

    self.assertEqual(("key2", ["c", "d", "e", "f"]), next(i))
    self.assertIsNone(
        input_readers._ReducerReader.decode_data(
            reader.to_json()["current_values"]))
    self.assertIsNone(
        input_readers._ReducerReader.decode_data(
            reader.to_json()["current_key"]))

    try:
      next(i)
      self.fail("Exception expected")
    except StopIteration:
      pass

  def testSerialization(self):
    self.skipTest("ReducerReader serialization loses file position with cloud storage SDK")


class GoogleCloudStorageInputTestBase(testutil.CloudStorageTestBase, testutil.HandlerTestBase):

  NUM_SHARDS = 10

  def create_mapper_spec(self, num_shards=None, input_params=None):
    mapper_spec = model.MapperSpec(
        "DummyHandler",
        self.READER_NAME,
        {"input_reader": input_params or {}},
        num_shards or self.NUM_SHARDS)
    return mapper_spec

  def create_test_file(self, filename, content):
    blob = self.bucket.blob(filename)
    blob.upload_from_string(content)


class GoogleCloudStorageInputReaderWithDelimiterTest(
    GoogleCloudStorageInputTestBase):

  READER_CLS = input_readers.GoogleCloudStorageInputReader
  READER_NAME = input_readers.__name__ + "." + READER_CLS.__name__

  def setUp(self):
    super().setUp()

    self.test_num_files = 20
    self.test_filenames = []
    for file_num in range(self.test_num_files):
      filename = f"{self.gcsPrefix}/file-{file_num:03d}"
      self.test_filenames.append(filename)
      self.create_test_file(filename, "foo")

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
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/dir-0*", f"{self.gcsPrefix}/file*"],
                                              "delimiter": "/"}))
    self.assertEqual(1, len(readers))
    reader = readers[0]
    self.assertEqual(10, len(reader._filenames))
    result_filenames = []
    while True:
      reader = self.READER_CLS.from_json_str(reader.to_json_str())
      try:
        result_filenames.append(next(reader)._blob.name)
      except StopIteration:
        break
    self.assertEqual(self.file_per_dir * 10,
                     len(result_filenames))
    self.assertEqual(self.filenames_in_first_10_dirs,
                     result_filenames)

  def testFailOnMissingInputSerialization(self):
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
    self.assertFalse(reader._fail_on_missing_input)


  def testSplit(self):
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

    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/*"],
                                              "delimiter": "/"}))
    self.assertEqual(1, len(readers))
    reader = readers[0]
    self.assertEqual(self.dirs, len(reader._filenames))
    self.assertEqual(self.file_per_dir * self.dirs,
                     len([f for f in reader]))


class GoogleCloudStorageInputReaderTest(GoogleCloudStorageInputTestBase):

  READER_CLS = input_readers.GoogleCloudStorageInputReader
  READER_NAME = input_readers.__name__ + "." + READER_CLS.__name__

  def setUp(self):
    super().setUp()

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
    self.READER_CLS.validate(
        self.create_mapper_spec(input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": ["1"]}))

  def testValidate_PassesWithBucketFromMapperSpec(self):
    self.READER_CLS.validate(
        model.MapperSpec(
            "DummyHandler",
            self.READER_NAME,
            {"bucket_name": self.TEST_BUCKET,
             "input_reader": {"objects": ["1", "2", "3"]}},
            self.NUM_SHARDS))

  def testGetParams_DoesntOverwriteInputReaderBucket(self):
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
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/file-*"]}))
    self.assertEqual(1, len(readers))
    self.assertEqual(self.test_num_files, len(readers[0]._filenames))

    self.assertTrue(self.test_num_files > 10,
                    msg="More than 10 files required for testing")
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/file-00*"]}))
    self.assertEqual(1, len(readers))
    self.assertEqual(10, len(readers[0]._filenames))

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
    self.assertTrue(shard_state.counters_map.get(
        input_readers.COUNTER_IO_READ_MSEC) > 0)

  def testNextWithMissingFiles(self):
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.TEST_BUCKET,
                                              "objects": [f"{self.gcsPrefix}/file-*"]}))
    self.assertEqual(1, len(readers))

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
    reader = self.READER_CLS.from_json(readers[0].to_json())

    found_content = []
    for _ in range(self.test_num_files):
      reader_file = next(reader)
      found_content.append(reader_file.read())
      reader = self.READER_CLS.from_json(reader.to_json())
    self.assertEqual(len(self.test_content), len(found_content))
    for content in self.test_content:
      self.assertTrue(content.encode() in found_content)

    self.assertRaises(StopIteration, reader.__next__)


class GoogleCloudStorageRecordInputReaderTest(GoogleCloudStorageInputTestBase):

  READER_CLS = input_readers.GoogleCloudStorageRecordInputReader
  READER_NAME = input_readers.__name__ + "." + READER_CLS.__name__

  def create_test_file(self, filename, content):
    blob = self.bucket.blob(filename)
    test_file = blob.open("wb")
    with records.RecordsWriter(test_file) as w:
      for c in content:
        w.write(c)
    test_file.close()

  def _gcs_path(self, filename):
    return f"/{self.TEST_BUCKET}/{filename}"

  def testSingleFileNoRecord(self):
    filename = f"{self.gcsPrefix}/empty-file"
    self.create_test_file(filename, [])
    reader = self.READER_CLS([self._gcs_path(filename)])

    with self.assertRaises(StopIteration):
      next(reader)

  def testSingleFileOneRecord(self):
    filename = f"{self.gcsPrefix}/single-record-file"
    data = b"foobardata"
    self.create_test_file(filename, [data])
    reader = self.READER_CLS([self._gcs_path(filename)])

    self.assertEqual(data, next(reader))
    self.assertRaises(StopIteration, reader.__next__)

  def testSingleFileManyRecords(self):
    filename = f"{self.gcsPrefix}/many-records-file"
    data = []
    for record_num in range(100):
      data.append((b"%03d" % record_num) * 10)
    self.create_test_file(filename, data)
    reader = self.READER_CLS([self._gcs_path(filename)])

    for record in data:
      self.assertEqual(record, next(reader))
    self.assertRaises(StopIteration, reader.__next__)
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
    self.assertRaises(StopIteration, reader.__next__)

  def testManyFilesManyRecords(self):
    filenames = []
    all_data = []
    for file_num in range(10):
      filename = f"{self.gcsPrefix}/file-{file_num:03d}"
      data_set = []
      for record_num in range(10):
        data_set.append((b"%03d" % record_num) * 10)
      self.create_test_file(filename, data_set)
      filenames.append(self._gcs_path(filename))
      all_data.append(data_set)
    reader = self.READER_CLS(filenames)

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, next(reader))
    self.assertRaises(StopIteration, reader.__next__)

  def testManyFilesSomeEmpty(self):
    filenames = []
    all_data = []
    for file_num in range(50):
      filename = f"{self.gcsPrefix}/file-{file_num:03d}"
      data_set = []
      for record_num in range(file_num % 5):
        data_set.append((b"%03d" % record_num) * 10)
      self.create_test_file(filename, data_set)
      filenames.append(self._gcs_path(filename))
      all_data.append(data_set)
    reader = self.READER_CLS(filenames)

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, next(reader))
    self.assertRaises(StopIteration, reader.__next__)

  def testSerialization(self):
    self.skipTest("Record reader serialization loses file position with cloud storage SDK")

  def testCounters(self):
    filenames = []
    all_data = []
    for file_num in range(10):
      filename = f"{self.gcsPrefix}/file-{file_num:03d}"
      data_set = []
      for record_num in range(10):
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

    self.assertEqual(0, shard_state.counters_map.get(
        input_readers.COUNTER_IO_READ_MSEC))
    self.assertEqual(0, shard_state.counters_map.get(
        input_readers.COUNTER_IO_READ_BYTES))

    reader = self.READER_CLS([self._gcs_path(f) for f in filenames])

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, next(reader))
    self.assertRaises(StopIteration, reader.__next__)

    self.assertIsNotNone(shard_state.counters_map.get(
        input_readers.COUNTER_IO_READ_MSEC))
    self.assertEqual(10 * 10 * 30, shard_state.counters_map.get(
        input_readers.COUNTER_IO_READ_BYTES))
