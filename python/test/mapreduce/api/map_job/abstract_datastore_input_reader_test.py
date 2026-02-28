#!/usr/bin/env python
"""Abstract Datastore Input Reader Test Helper for the map_job API."""
import os
import unittest

from mapreduce.compat import key_range
from mapreduce import errors
from mapreduce import model
from testlib import testutil
from mapreduce.api import map_job
from mapreduce.api.map_job import abstract_datastore_input_reader

# pylint: disable=invalid-name


class AbstractDatastoreInputReaderTest(testutil.CloudStorageTestBase, unittest.TestCase):

  @property
  def reader_cls(self):
    return abstract_datastore_input_reader.AbstractDatastoreInputReader

  def setUp(self):
    self.appid = "testbed-test"
    super().setUp()

  def testValidate_Passes(self):
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.reader_cls.validate(conf)

  def testValidate_NoEntityFails(self):
    params = {}
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.assertRaises(errors.BadReaderParamsError,
                      self.reader_cls.validate,
                      conf)

  def testValidate_EntityKindWithNoModel(self):
    params = {
        "entity_kind": "foo",
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.reader_cls.validate(conf)

  def testValidate_BadBatchSize(self):
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        "batch_size": "xxx"
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.assertRaises(errors.BadReaderParamsError,
                      self.reader_cls.validate,
                      conf)
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        "batch_size": "0"
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.assertRaises(errors.BadReaderParamsError,
                      self.reader_cls.validate,
                      conf)

    params = {
        "entity_kind": testutil.ENTITY_KIND,
        "batch_size": "-1"
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.assertRaises(errors.BadReaderParamsError,
                      self.reader_cls.validate,
                      conf)

  def testValidate_WrongTypeNamespace(self):
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        "namespace": 5
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.assertRaises(errors.BadReaderParamsError,
                      self.reader_cls.validate,
                      conf)

  def testChooseSplitPoints(self):
    self.assertEqual(
        [5],
        self.reader_cls._choose_split_points(
            sorted([0, 9, 8, 7, 1, 2, 3, 4, 5, 6]), 2))

    self.assertEqual(
        [3, 7],
        self.reader_cls._choose_split_points(
            sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 3))

    self.assertEqual(
        list(range(1, 10)),
        self.reader_cls._choose_split_points(
            sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 10))

    self.assertRaises(
        AssertionError,
        self.reader_cls._choose_split_points,
        sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 11)

  def _assertEquals_splitNSByScatter(self, shards, expected, ns=""):
    results = self.reader_cls._split_ns_by_scatter(
        shards, ns, "TestEntity", self.appid)
    self.assertEqual(expected, results)

  def testSplitNSByScatter_NotEnoughData(self):
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
        self.reader_cls._to_key_ranges_by_shard(
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
    kranges_by_shard = (self.reader_cls._to_key_ranges_by_shard(
        self.appid, namespaces, shards,
        model.QuerySpec(entity_kind="TestEntity")))
    self.assertEqual(shards, len(kranges_by_shard))

    expected.sort()
    results = []
    for kranges in kranges_by_shard:
      results.extend(list(kranges))
    results.sort()
    self.assertEqual(expected, results)
