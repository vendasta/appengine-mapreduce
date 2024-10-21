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

"""Utilities to aid in testing mapreduces."""



import base64
import collections
import logging
import os
import re

from flask import Flask
from werkzeug.routing import RequestRedirect

import mapreduce
from mapreduce import model

# TODO(user): Add tests for this file.

# Change level to logging.DEBUG to see stacktrack on failed task executions.
_LOGGING_LEVEL = logging.ERROR
logging.getLogger().setLevel(_LOGGING_LEVEL)


def decode_task_payload(task):
  """Decodes POST task payload.

  This can only decode POST payload for a normal task. For huge task,
  use model.HugeTask.decode_payload.

  Args:
    task: a dict representing a taskqueue task as documented in taskqueue_stub.

  Returns:
    parameter_name -> parameter_value dict. If multiple parameter values are
    present, then parameter_value will be a list.
  """
  if not task:
    return {}
  # taskqueue_stub base64 encodes body when it returns the task to us.
  body = base64.b64decode(task["body"]).decode()
  # pylint: disable=protected-access
  return model.HugeTask._decode_payload(body)

def execute_task(task, retries=0, handlers_map=None):
  if not handlers_map:
    handlers_map = mapreduce.create_handlers_map()
  
  app = Flask(__name__)
  for pattern, handler_class in handlers_map:
    app.add_url_rule(pattern, view_func=handler_class.as_view(pattern.lstrip("/")))

  name = task['name']
  method = task['method']
  url = task['url']
  headers = dict(task['headers'])
  data = base64.b64decode(task['body'])

  headers.update({
    'X-AppEngine-TaskName': name,
    'X-AppEngine-QueueName': task.get("queue_name", "default"),
    'X-AppEngine-TaskExecutionCount': retries,
  })

  os.environ["HTTP_X_APPENGINE_QUEUENAME"] = task.get("queue_name", "default")

  logging.debug('Executing "%s %s" name="%s"', method, url, name)
  client = app.test_client()
  result = client.open(url, method=method, headers=headers, data=data)
  if result.status_code != 200:
    raise Exception('Task failed with status %s' % result.status_code)
  
  rule, _ = app.url_map.bind('').match(url, method=method, return_rule=True)

  return app.view_functions[rule.endpoint].view_class


def execute_all_tasks(taskqueue, queue="default", handlers_map=None):
  """Run and remove all tasks in the taskqueue.

  Args:
    taskqueue: An instance of taskqueue stub.
    queue: Queue name to run all tasks from.
    hanlders_map: see main.create_handlers_map.

  Returns:
    task_run_counts: a dict from handler class to the number of tasks
      it handled.
  """
  tasks = taskqueue.GetTasks(queue)
  taskqueue.FlushQueue(queue)
  task_run_counts = collections.defaultdict(int)
  for task in tasks:
    retries = 0
    while True:
      try:
        handler = execute_task(task, retries, handlers_map=handlers_map)
        task_run_counts[handler] += 1
        break
      # pylint: disable=broad-except
      except Exception as e:
        retries += 1
        # Arbitrary large number.
        if retries > 100:
          logging.debug("Task %s failed for too many times. Giving up.",
                        task["name"])
          raise
        logging.debug(
            "Task %s is being retried for the %s time",
            task["name"],
            retries)
        logging.debug(e)

  return task_run_counts


def execute_until_empty(taskqueue, queue="default", handlers_map=None):
  """Execute taskqueue tasks until it becomes empty.

  Args:
    taskqueue: An instance of taskqueue stub.
    queue: Queue name to run all tasks from.
    hanlders_map: see main.create_handlers_map.

  Returns:
    task_run_counts: a dict from handler class to the number of tasks
      it handled.
  """
  task_run_counts = collections.defaultdict(int)
  while taskqueue.GetTasks(queue):
    new_counts = execute_all_tasks(taskqueue, queue, handlers_map)
    for handler_cls in new_counts:
      task_run_counts[handler_cls] += new_counts[handler_cls]
  return task_run_counts
