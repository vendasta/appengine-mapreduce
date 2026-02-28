#!/usr/bin/env python
"""Datastore Input Reader tests for the map_job API."""
import datetime

from mapreduce import errors
from testlib import testutil
from mapreduce.api import map_job
import datastore_input_reader_base_test
from mapreduce.api.map_job import datastore_input_reader


class DatastoreInputReaderTest(testutil.CloudStorageTestBase, datastore_input_reader_base_test.DatastoreInputReaderBaseTest):

  @property
  def reader_cls(self):
    return datastore_input_reader.DatastoreInputReader

  def testValidate_Filters(self):
    params = {
        "entity_kind": self.entity_kind,
        "filters": [("a", "=", 1), ("b", "=", 2)],
        }
    new = datetime.datetime.now()
    old = new.replace(year=new.year-1)
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    conf.input_reader_cls.validate(conf)

    params["filters"] = [["datetime_property", ">", old],
                         ["datetime_property", "<=", new],
                         ["a", "=", 1]]
    self.assertRaises(errors.BadReaderParamsError,
                      conf.input_reader_cls.validate,
                      conf)

  def testEntityKindWithDot(self):
    self._create_entities(list(range(3)), {"1": 1}, "", testutil.TestEntityWithDot)

    params = {
        "entity_kind": testutil.TestEntityWithDot._get_kind(),
        "namespace": "",
        }
    conf = map_job.JobConfig(
        job_name=self.gcsPrefix,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=2)
    results = conf.input_reader_cls.split_input(conf)
    self.assertGreaterEqual(len(results), 1)
    self._assertEqualsForAllShards_splitInput(["0", "1", "2"], None, *results)
