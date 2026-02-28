#!/usr/bin/env python
import os
import unittest

from google.cloud import ndb
from google.cloud import storage

from mapreduce import json_util
from mapreduce import model
import mapreduce

from flask import Flask

from pipeline import taskqueue_compat
from pipeline import taskqueue_test_stub

_storage_client = storage.Client()


class TestJsonType:

  def __init__(self, size=0):
    self.size = size

  def to_json(self):
    return {"size": self.size}

  @classmethod
  def from_json(cls, json):
    return cls(json["size"])


class TestEntity(ndb.Model):
  json_property = json_util.JsonProperty(TestJsonType)
  json_property_default_value = json_util.JsonProperty(
      TestJsonType, default=TestJsonType())
  int_property = ndb.IntegerProperty()
  datetime_property = ndb.DateTimeProperty(auto_now=True)

  a = ndb.IntegerProperty()
  b = ndb.IntegerProperty()


class NdbTestEntity(ndb.Model):
  datetime_property = ndb.DateTimeProperty(auto_now=True)

  a = ndb.IntegerProperty()
  b = ndb.IntegerProperty()


class TestEntityWithDot(ndb.Model):

  @classmethod
  def _get_kind(cls):
    return "Test.Entity.With.Dot"


ENTITY_KIND = "testlib.testutil.TestEntity"
NDB_ENTITY_KIND = "testlib.testutil.NdbTestEntity"


def key(entity_id, namespace=None, kind="TestEntity"):
  return ndb.Key(kind, entity_id, namespace=namespace)


def _create_entities(keys_itr,
                     key_to_scatter_val,
                     ns=None,
                     entity_model_cls=TestEntity):
  entities = []
  for k in keys_itr:
    entity = entity_model_cls(id=str(k), namespace=ns)
    entities.append(entity)
    entity.put()
  return entities


class HandlerTestBase(unittest.TestCase):

  MAPREDUCE_URL = "/_ah/mapreduce/kickoffjob_callback"

  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testbed-test"
    self.major_version_id = "1"
    self.version_id = self.major_version_id + ".23456789"
    self.module_id = "foo_module"
    self.host = "{}.{}.{}".format(
        self.major_version_id, self.module_id, "testbed-test.appspot.com")

    os.environ["GOOGLE_CLOUD_PROJECT"] = self.appid
    os.environ["GAE_VERSION"] = self.version_id
    os.environ["GAE_SERVICE"] = self.module_id
    os.environ["DEFAULT_VERSION_HOSTNAME"] = "%s.appspot.com" % self.appid
    os.environ["HTTP_HOST"] = self.host

    taskqueue_compat.set_test_mode(True)
    taskqueue_test_stub.reset_test_stub()
    self.taskqueue = taskqueue_test_stub.get_test_stub()

    handlers_map = mapreduce.create_handlers_map()
    self.app = Flask(__name__)
    for pattern, handler_class in handlers_map:
      self.app.add_url_rule(pattern, view_func=handler_class.as_view(pattern.lstrip("/")))
    self.client = self.app.test_client()

  def tearDown(self):
    for key in ("GOOGLE_CLOUD_PROJECT", "GAE_VERSION", "GAE_SERVICE",
                "DEFAULT_VERSION_HOSTNAME", "HTTP_HOST",
                "HTTP_X_APPENGINE_QUEUENAME"):
      os.environ.pop(key, None)

    unittest.TestCase.tearDown(self)

  def assertTaskStarted(self, queue="default"):
    tasks = self.taskqueue.get_tasks(queue)
    self.assertEqual(1, len(tasks))
    self.assertEqual(tasks[0]["url"], self.MAPREDUCE_URL)

  def create_shard_state(self, shard_number):
    shard_state = model.ShardState.create_new("DummyMapReduceJobId",
                                              shard_number)
    shard_state.put()
    return shard_state


class CloudStorageTestBase(HandlerTestBase):

  TEST_BUCKET = "byates"
  TEST_TMP_BUCKET = "byates"

  def setUp(self):
    for blob in self.bucket.list_blobs(prefix=self.gcsPrefix):
      blob.delete()
    return super().setUp()

  @property
  def gcsPrefix(self):
    return f"{self.__module__}/{self.__class__.__name__}/{self._testMethodName}"

  @property
  def bucket(self):
    return _storage_client.get_bucket(self.TEST_BUCKET)

  @property
  def tmp_bucket(self):
    return _storage_client.get_bucket(self.TEST_TMP_BUCKET)
