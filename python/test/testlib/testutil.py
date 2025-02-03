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

"""Test utilities for mapreduce framework."""



# pylint: disable=g-bad-name
# pylint: disable=g-import-not-at-top
# pylint: disable=invalid-import-order

# os_compat must be first to ensure timezones are UTC.
# Disable "unused import" and "invalid import order"
# pylint: disable=unused-import,g-bad-import-order
from google.appengine.tools import os_compat
# pylint: enable=unused-import,g-bad-import-order

import os
import unittest

from google.appengine.ext import ndb
from google.appengine.api import datastore_types
from google.appengine.api import namespace_manager
from google.appengine.api import queueinfo
from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import db
from google.appengine.ext import testbed
from google.appengine.datastore import entity_bytes_pb2 as entity_pb
from mapreduce import json_util
from mapreduce import model
import mapreduce

from flask import Flask

from google.cloud import storage

_storage_client = storage.Client()


class TestJsonType:
  """Test class with to_json/from_json methods."""

  def __init__(self, size=0):
    self.size = size

  def to_json(self):
    return {"size": self.size}

  @classmethod
  def from_json(cls, json):
    return cls(json["size"])


class TestEntity(db.Model):
  """Test entity class."""

  json_property = json_util.JsonProperty(TestJsonType)
  json_property_default_value = json_util.JsonProperty(
      TestJsonType, default=TestJsonType())
  int_property = db.IntegerProperty()
  datetime_property = db.DateTimeProperty(auto_now=True)

  a = db.IntegerProperty()
  b = db.IntegerProperty()


class NdbTestEntity(ndb.Model):
  datetime_property = ndb.DateTimeProperty(auto_now=True)

  a = ndb.IntegerProperty()
  b = ndb.IntegerProperty()


class TestEntityWithDot(db.Model):
  """Test entity class with dot in its kind."""

  @classmethod
  def kind(cls):
    return "Test.Entity.With.Dot"


ENTITY_KIND = "testlib.testutil.TestEntity"
NDB_ENTITY_KIND = "testlib.testutil.NdbTestEntity"


def key(entity_id, namespace=None, kind="TestEntity"):
  """Create a key for TestEntity with specified id.

  Used to shorten expected data.

  Args:
    entity_id: entity id
    namespace: the namespace
    kind: the kind
  Returns:
    db.Key instance with specified id for TestEntity.
  """
  return db.Key.from_path(kind, entity_id, namespace=namespace)


def set_scatter_setter(key_names_to_vals):
  """Monkey patch the scatter property setter.

  Args:
    key_names_to_vals: a dict from key_name to scatter property value.
      The value will be casted to str as the scatter property is. Entities
      who key is in the map will have the corresponding scatter property.
  """

  def _get_scatter_property(entity_proto):
    key_name = entity_proto.key.path.element[-1].name
    if key_name in key_names_to_vals:
      scatter_property = entity_pb.Property()
      for field_name in scatter_property.DESCRIPTOR.fields_by_name:
        print(field_name)
      scatter_property.name = datastore_types.SCATTER_SPECIAL_PROPERTY
      scatter_property.meaning = entity_pb.Property.BYTESTRING
      scatter_property.multiple = False
      scatter_property.value.stringValue = datastore_types.ByteString(str(key_names_to_vals[key_name]).encode())
      return scatter_property
  datastore_stub_util._SPECIAL_PROPERTY_MAP[
      datastore_types.SCATTER_SPECIAL_PROPERTY] = (
          False, True, _get_scatter_property)


def _create_entities(keys_itr,
                     key_to_scatter_val,
                     ns=None,
                     entity_model_cls=TestEntity):
  """Create entities for tests.

  Args:
    keys_itr: an iterator that contains all the key names.
    key_to_scatter_val: a dict that maps key names to its scatter values.
    ns: the namespace to create the entity at.
    entity_model_cls: entity model class.

  Returns:
    A list of entities created.
  """
  namespace_manager.set_namespace(ns)
  set_scatter_setter(key_to_scatter_val)
  entities = []
  for k in keys_itr:
    entity = entity_model_cls(key_name=str(k))
    entities.append(entity)
    entity.put()
  namespace_manager.set_namespace(None)
  return entities


class HandlerTestBase(unittest.TestCase):
  """Base class for all webapp.RequestHandler tests."""

  MAPREDUCE_URL = "/_ah/mapreduce/kickoffjob_callback"

  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testapp"
    self.major_version_id = "1"
    self.version_id = self.major_version_id + ".23456789"
    self.module_id = "foo_module"
    self.host = "{}.{}.{}".format(
        self.major_version_id, self.module_id, "testapp.appspot.com")

    self.testbed = testbed.Testbed()
    self.testbed.activate()

    os.environ["APPLICATION_ID"] = self.appid
    os.environ["GAE_VERSION"] = self.version_id
    os.environ["GAE_SERVICE"] = self.module_id
    os.environ["DEFAULT_VERSION_HOSTNAME"] = "%s.appspot.com" % self.appid
    os.environ["HTTP_HOST"] = self.host

    self.testbed.init_app_identity_stub()
    self.testbed.init_blobstore_stub()
    # HRD with no eventual consistency.
    policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
    self.testbed.init_datastore_v3_stub(consistency_policy=policy)
    self.testbed.init_memcache_stub()
    self.testbed.init_taskqueue_stub()
    self.testbed.init_urlfetch_stub()
    self.testbed.init_user_stub()

    # For backwards compatibility, maintain easy references to some stubs
    self.taskqueue = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

    self.taskqueue.queue_yaml_parser = (
        # pylint: disable=g-long-lambda
        lambda x: queueinfo.LoadSingleQueue(
            "queue:\n"
            "- name: default\n"
            "  rate: 10/s\n"
            "- name: crazy-queue\n"
            "  rate: 2000/d\n"
            "  bucket_size: 10\n"))
    
    handlers_map = mapreduce.create_handlers_map()
    self.app = Flask(__name__)
    for pattern, handler_class in handlers_map:
      self.app.add_url_rule(pattern, view_func=handler_class.as_view(pattern.lstrip("/")))
    self.client = self.app.test_client()

  def tearDown(self):
    del os.environ["APPLICATION_ID"]
    del os.environ["GAE_VERSION"]
    del os.environ["GAE_SERVICE"]
    del os.environ["DEFAULT_VERSION_HOSTNAME"]
    del os.environ["HTTP_HOST"]

    self.testbed.deactivate()

    unittest.TestCase.tearDown(self)

  def assertTaskStarted(self, queue="default"):
    tasks = self.taskqueue.GetTasks(queue)
    self.assertEqual(1, len(tasks))
    self.assertEqual(tasks[0]["url"], self.MAPREDUCE_URL)

  def create_shard_state(self, shard_number):
    """Create a model.ShardState.

    Args:
      shard_number: The index for this shard (zero-indexed).

    Returns:
      a model.ShardState for the given shard.
    """
    shard_state = model.ShardState.create_new("DummyMapReduceJobId",
                                              shard_number)
    shard_state.put()
    return shard_state


class CloudStorageTestBase(HandlerTestBase):
  """Test base class that ensures cloudstorage library is available."""

  TEST_BUCKET = "byates"
  TEST_TMP_BUCKET = "byates"

  @property
  def gcsPrefix(self):
    return f"{self.__module__}/{self.__class__.__name__}/{self._testMethodName}"

  @property
  def bucket(self):
    return _storage_client.get_bucket(self.TEST_BUCKET)
  
  @property
  def tmp_bucket(self):
    return _storage_client.get_bucket(self.TEST_TMP_BUCKET)
