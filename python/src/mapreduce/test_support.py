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
import time

from flask import Flask
from werkzeug.routing import RequestRedirect

import mapreduce
from mapreduce import model

# TODO(user): Add tests for this file.

# Change level to logging.DEBUG to see stacktrack on failed task executions.
_LOGGING_LEVEL = logging.ERROR
logging.getLogger().setLevel(_LOGGING_LEVEL)

# Maximum number of retries for a task
MAX_TASK_RETRIES = 3
# Delay between retries in seconds
RETRY_DELAY = 0.1
# Maximum number of empty task queue checks
MAX_EMPTY_CHECKS = 10
# Delay between empty checks in seconds
EMPTY_CHECK_DELAY = 0.1

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

def create_test_app(handlers_map=None):
  """Creates a Flask test app with the mapreduce handlers."""
  if not handlers_map:
    handlers_map = mapreduce.create_handlers_map()
  
  app = Flask(__name__)
  for pattern, handler_class in handlers_map:
    app.add_url_rule(pattern, view_func=handler_class.as_view(pattern.lstrip("/")))
  return app

def execute_task(task, retries=0, app=None, handlers_map=None):
  """Execute a single task.
  
  Args:
    task: Task dict to execute
    retries: Number of previous retries
    app: Flask app to use (will create if None)
    handlers_map: Handler map to use if creating new app
    
  Returns:
    Handler class that executed the task
  
  Raises:
    Exception if task fails after max retries
  """
  if not app:
    app = create_test_app(handlers_map)

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

def execute_all_tasks(taskqueue, queue="default", app=None, handlers_map=None):
  """Run and remove all tasks in the taskqueue.

  Args:
    taskqueue: An instance of taskqueue stub.
    queue: Queue name to run all tasks from.
    app: Flask app to use (will create if None)
    handlers_map: Handler map to use if creating new app

  Returns:
    task_run_counts: a dict from handler class to the number of tasks
      it handled.
  """
  if not app:
    app = create_test_app(handlers_map)

  tasks = taskqueue.GetTasks(queue)
  taskqueue.FlushQueue(queue)
  task_run_counts = collections.defaultdict(int)
  
  for task in tasks:
    retries = 0
    while True:
      try:
        handler = execute_task(task, retries, app=app)
        task_run_counts[handler] += 1
        break
      except Exception as e:
        retries += 1
        if retries > MAX_TASK_RETRIES:
          logging.error("Task %s failed after %d retries", task["name"], retries)
          raise
        logging.debug(
            "Task %s is being retried for the %s time",
            task["name"],
            retries)
        logging.debug(e)
        time.sleep(RETRY_DELAY)

  return task_run_counts

def execute_until_empty(taskqueue, queue="default", handlers_map=None):
  """Execute taskqueue tasks until it becomes empty.

  Args:
    taskqueue: An instance of taskqueue stub.
    queue: Queue name to run all tasks from.
    handlers_map: Handler map to use if creating new app

  Returns:
    task_run_counts: a dict from handler class to the number of tasks
      it handled.
  """
  app = create_test_app(handlers_map)
  task_run_counts = collections.defaultdict(int)
  empty_checks = 0
  
  while empty_checks < MAX_EMPTY_CHECKS:
    tasks = taskqueue.GetTasks(queue)
    if not tasks:
      empty_checks += 1
      time.sleep(EMPTY_CHECK_DELAY)
      continue
      
    empty_checks = 0
    new_counts = execute_all_tasks(taskqueue, queue, app=app)
    for handler_cls in new_counts:
      task_run_counts[handler_cls] += new_counts[handler_cls]
      
  return task_run_counts
