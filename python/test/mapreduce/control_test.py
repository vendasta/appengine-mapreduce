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

import datetime
import os
import random
import string
import time
import unittest

from google.appengine.ext import db

from mapreduce import control
from mapreduce import hooks
from mapreduce import model
from mapreduce import test_support
from testlib import testutil
from mapreduce.api import map_job


def random_string(length):
  """Generate a random string of given length."""
  return "".join(
      random.choice(string.ascii_letters + string.digits) for _ in range(length))


class FakeEntity(db.Model):
  """Test entity class."""


class FakeHooks(hooks.Hooks):
  """Test hooks class."""

  enqueue_kickoff_task_calls = []

  def enqueue_kickoff_task(self, task, queue_name):
    FakeHooks.enqueue_kickoff_task_calls.append((task, queue_name))


def fake_handler(entity):
  """Test handler function."""
  pass


class ControlTest(testutil.CloudStorageTestBase, testutil.HandlerTestBase):
  """Tests for control module."""

  QUEUE_NAME = "crazy-queue"

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    FakeHooks.enqueue_kickoff_task_calls = []

  def get_mapreduce_spec(self, task):
    """Get mapreduce spec form kickoff task payload."""
    payload = test_support.decode_task_payload(task)
    return model.MapreduceSpec.from_json_str(payload["mapreduce_spec"])

  def validate_map_started(self, mapreduce_id, queue_name=None):
    """Tests that the map has been started."""
    queue_name = queue_name or self.QUEUE_NAME
    self.assertTrue(mapreduce_id)

    # Note: only a kickoff job is pending at this stage, shards come later.
    tasks = self.taskqueue.GetTasks(queue_name)
    self.assertEqual(1, len(tasks))
    # Checks that tasks are scheduled into the future.
    task = tasks[0]
    self.assertEqual("/mapreduce/kickoffjob_callback/" + mapreduce_id,
                     task["url"])
    handler = test_support.execute_task(task)
    state = model.MapreduceState.get_by_job_id(mapreduce_id)
    params = map_job.JobConfig._get_default_mr_params()
    params.update({"foo": "bar",
                   "queue_name": queue_name})
    self.assertEqual(state.mapreduce_spec.params, params)

    job_config = map_job.JobConfig._to_map_job_config(state.mapreduce_spec,
                                                      queue_name)
    self.assertEqual(0, job_config._api_version)
    return task["eta"]

  def testStartMap(self):
    """Test start_map function.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    FakeEntity().put()

    shard_count = 4
    mapreduce_id = control.start_map(
        self.gcsPrefix,
        __name__ + ".fake_handler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
        },
        shard_count,
        mapreduce_parameters={"foo": "bar"},
        queue_name=self.QUEUE_NAME)

    self.validate_map_started(mapreduce_id)

  def testStartMap_Countdown(self):
    """Test that MR can be scheduled into the future.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    FakeEntity().put()

    # MR should be scheduled into the future.
    now_sec = int(time.time())

    shard_count = 4
    mapreduce_id = control.start_map(
        self.gcsPrefix,
        __name__ + ".fake_handler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
        },
        shard_count,
        mapreduce_parameters={"foo": "bar"},
        queue_name=self.QUEUE_NAME,
        countdown=1000)

    task_eta = self.validate_map_started(mapreduce_id)
    eta_sec = time.mktime(time.strptime(task_eta, "%Y/%m/%d %H:%M:%S"))
    self.assertTrue(now_sec + 1000 <= eta_sec)

  def testStartMap_Eta(self):
    """Test that MR can be scheduled into the future.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    FakeEntity().put()

    # MR should be scheduled into the future.
    eta = datetime.datetime.utcnow() + datetime.timedelta(hours=1)

    shard_count = 4
    mapreduce_id = control.start_map(
        self.gcsPrefix,
        __name__ + ".fake_handler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
        },
        shard_count,
        mapreduce_parameters={"foo": "bar"},
        queue_name=self.QUEUE_NAME,
        eta=eta)

    task_eta = self.validate_map_started(mapreduce_id)
    self.assertEqual(eta.strftime("%Y/%m/%d %H:%M:%S"), task_eta)

  def testStartMap_QueueEnvironment(self):
    """Test that the start_map inherits its queue from the enviornment."""
    FakeEntity().put()

    shard_count = 4
    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = self.QUEUE_NAME
    try:
      mapreduce_id = control.start_map(
          "fake_map",
          __name__ + ".fake_handler",
          "mapreduce.input_readers.DatastoreInputReader",
          {
              "entity_kind": __name__ + "." + FakeEntity.__name__,
          },
          shard_count,
          mapreduce_parameters={"foo": "bar"},
          )
    finally:
      del os.environ["HTTP_X_APPENGINE_QUEUENAME"]

    self.validate_map_started(mapreduce_id)

  def testStartMap_Hooks(self):
    """Tests that MR can be scheduled with a hook class installed.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    FakeEntity().put()

    shard_count = 4
    mapreduce_id = control.start_map(
        "fake_map",
        __name__ + ".fake_handler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
        },
        shard_count,
        mapreduce_parameters={"foo": "bar"},
        queue_name="crazy-queue",
        hooks_class_name=__name__+"."+FakeHooks.__name__)

    self.assertTrue(mapreduce_id)
    task, queue_name = FakeHooks.enqueue_kickoff_task_calls[0]
    self.assertEqual("/mapreduce/kickoffjob_callback/" + mapreduce_id,
                     task.url)
    self.assertEqual("crazy-queue", queue_name)

  def testStartMap_RaisingHooks(self):
    """Tests that MR can be scheduled with a dummy hook class installed.

    The dummy hook class raises NotImplementedError for all method calls so the
    default scheduling logic should be used.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    FakeEntity().put()

    shard_count = 4
    mapreduce_id = control.start_map(
        "fake_map",
        __name__ + ".fake_handler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
        },
        shard_count,
        mapreduce_parameters={"foo": "bar"},
        queue_name="crazy-queue",
        hooks_class_name=hooks.__name__+"."+hooks.Hooks.__name__)

    self.validate_map_started(mapreduce_id)

  def testStartMap_HugePayload(self):
    """Test start_map function.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    FakeEntity().put()

    shard_count = 4
    mapreduce_id = ""

    mapreduce_id = control.start_map(
        "fake_map",
        __name__ + ".fake_handler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + FakeEntity.__name__,
            "huge_parameter": random_string(900000)
        },
        shard_count,
        mapreduce_parameters={"foo": "bar"},
        queue_name=self.QUEUE_NAME)
    self.validate_map_started(mapreduce_id)

  def testStartMapTransactional(self):
    """Test start_map function.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    FakeEntity().put()

    shard_count = 4
    mapreduce_id = ""

    @db.transactional(xg=True)
    def tx():
      some_entity = FakeEntity()
      some_entity.put()
      return control.start_map(
          self.gcsPrefix,
          __name__ + ".fake_handler",
          "mapreduce.input_readers.DatastoreInputReader",
          {
              "entity_kind": __name__ + "." + FakeEntity.__name__,
          },
          shard_count,
          mapreduce_parameters={"foo": "bar"},
          queue_name=self.QUEUE_NAME,
          in_xg_transaction=True)
    mapreduce_id = tx()
    self.validate_map_started(mapreduce_id)

  def testStartMapTransactional_HugePayload(self):
    """Test start_map function.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    FakeEntity().put()

    shard_count = 4
    mapreduce_id = ""

    @db.transactional(xg=True)
    def tx():
      some_entity = FakeEntity()
      some_entity.put()
      return control.start_map(
          self.gcsPrefix,
          __name__ + ".fake_handler",
          "mapreduce.input_readers.DatastoreInputReader",
          {
              "entity_kind": __name__ + "." + FakeEntity.__name__,
              "huge_parameter": random_string(900000)
          },
          shard_count,
          mapreduce_parameters={"foo": "bar"},
          queue_name=self.QUEUE_NAME,
          in_xg_transaction=True)
    mapreduce_id = tx()
    self.validate_map_started(mapreduce_id)


