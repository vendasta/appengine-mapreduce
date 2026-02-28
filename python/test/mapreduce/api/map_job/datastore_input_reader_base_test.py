#!/usr/bin/env python
"""Datastore Input Reader Base Test for the map_job API."""
import random
import unittest

from testlib import testutil
from mapreduce.api import map_job

# pylint: disable=invalid-name


class SkipTestsMeta(type):

  def __init__(cls, name, bases, dct):
    super().__init__(name, bases, dct)
    if cls.__name__ == "DatastoreInputReaderBaseTest":
      unittest.skip("Skip tests when testing from the base class.")(cls)
    else:
      cls.__unittest_skip__ = False
      cls.__unittest_skip_why__ = None


class DatastoreInputReaderBaseTest(unittest.TestCase, metaclass=SkipTestsMeta):

  @property
  def _create_entities(self):
    return testutil._create_entities

  @property
  def entity_kind(self):
    return "TestEntity"

  @property
  def reader_cls(self):
    raise NotImplementedError("reader_cls() not implemented in %s"
                              % self.__class__)

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
    self._original_max = (
        self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD)
    self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD = 2

  def tearDown(self):
    self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD = (
        self._original_max)

  def testSplitInput_withNs(self):
    self._create_entities(list(range(3)), {"1": 1}, "f")
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=2)
    results = self.reader_cls.split_input(conf)
    self.assertEqual(2, len(results))
    self._assertEqualsForAllShards_splitInput(["0", "1", "2"], None, *results)

  def testSplitInput_withNs_moreShardThanScatter(self):
    self._create_entities(list(range(3)), {"1": 1}, "f")
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=4)
    results = self.reader_cls.split_input(conf)
    self.assertTrue(len(results) >= 2)
    self._assertEqualsForAllShards_splitInput(["0", "1", "2"], None, *results)

  def testSplitInput_noEntity(self):
    params = {
        "entity_kind": self.entity_kind,
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    results = self.reader_cls.split_input(conf)
    self.assertEqual(None, results)

  def testSplitInput_moreThanOneNS(self):
    self._create_entities(list(range(3)), {"1": 1}, "1")
    self._create_entities(list(range(10, 13)), {"11": 11}, "2")
    params = {
        "entity_kind": self.entity_kind,
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=4)
    results = self.reader_cls.split_input(conf)
    self.assertTrue(len(results) >= 2)
    self._assertEqualsForAllShards_splitInput(
        ["0", "1", "2", "10", "11", "12"], None, *results)

  def testSplitInput_moreThanOneUnevenNS(self):
    self._create_entities(list(range(5)), {"1": 1, "3": 3}, "1")
    self._create_entities(list(range(10, 13)), {"11": 11}, "2")
    params = {
        "entity_kind": self.entity_kind,
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=4)
    results = self.reader_cls.split_input(conf)
    self.assertTrue(len(results) >= 2)
    self._assertEqualsForAllShards_splitInput(
        ["0", "1", "2", "3", "4", "10", "11", "12"], None, *results)

  def testSplitInput_lotsOfNS(self):
    random.seed(1)
    self._create_entities(list(range(3)), {"1": 1}, "9")
    self._create_entities(list(range(3, 6)), {"4": 4}, "_")
    self._create_entities(list(range(6, 9)), {"7": 7}, "a")
    params = {
        "entity_kind": self.entity_kind,
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=3)
    results = self.reader_cls.split_input(conf)
    self.assertEqual(3, len(results))
    self._assertEqualsForAllShards_splitInput(
        ["0", "1", "2", "3", "4", "5", "6", "7", "8"], None, *results)

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
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=shards)
    results = self.reader_cls.split_input(conf)
    self.assertEqual(shards, len(results))
    all_keys = empty_ns_keys + non_empty_ns_keys
    self._assertEqualsForAllShards_splitInput(all_keys,
                                              len(all_keys),
                                              *results)
