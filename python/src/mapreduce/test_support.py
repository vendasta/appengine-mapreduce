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
  body = base64.b64decode(task["body"])
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
  })

  logging.debug('Executing "%s %s" name="%s"', method, url, name)
  client = app.test_client()
  client.open(url, method=method, headers=headers, data=data)

  # url = task["url"]
  # handler = None

  # params = []

  # for (re_str, handler_class) in handlers_map:
  #   re_str = "^" + re_str + "($|\\?)"
  #   m = re.match(re_str, url)
  #   if m:
  #     params = m.groups()[:-1]
  #     break
  # else:
  #   raise Exception("Can't determine handler for %s" % task)

  # version = "mr-test-support-version.1"
  # module = "mr-test-support-module"
  # default_version_hostname = "mr-test-support.appspot.com"
  # host = "{}.{}.{}".format(version.split(".")[0],
  #              module,
  #              default_version_hostname)

  # os.environ.setdefault("CURRENT_VERSION_ID", version)
  # os.environ.setdefault("DEFAULT_VERSION_HOSTNAME", default_version_hostname)
  # os.environ.setdefault("CURRENT_MODULE_ID", module)
  # os.environ.setdefault("HTTP_HOST", host)

  # for k, v in task.get("headers", []):
  #   environ_key = "HTTP_" + k.replace("-", "_").upper()
  #   os.environ[environ_key] = v

  # os.environ["HTTP_X_APPENGINE_TASKEXECUTIONCOUNT"] = str(retries)
  # os.environ["HTTP_X_APPENGINE_TASKNAME"] = task.get("name", "default_task_name")
  # os.environ["HTTP_X_APPENGINE_QUEUENAME"] = task.get("queue_name", "default")
  # os.environ["PATH_INFO"] = task.get("path", "/") # TODO: check this

  # response = mock_webapp.MockResponse()
  # saved_os_environ = os.environ
  # copy_os_environ = dict(os.environ)
  # copy_os_environ.update(request.environ)

  # try:
  #   os.environ = copy_os_environ
  #   # Webapp2 expects request/response in the handler instantiation, and calls
  #   # initialize automatically.
  #   handler = handler_class(request, response)
  # except TypeError:
  #   # For webapp, setup request before calling initialize.
  #   handler = handler_class()
  #   handler.initialize(request, response)
  # finally:
  #   os.environ = saved_os_environ

  # try:
  #   os.environ = copy_os_environ




  # if task["method"] == "POST":
  #   request.data = base64.b64decode(task["body"])
  #   for k, v in decode_task_payload(task).items():
  #     request.form[k] = v

  # response = make_response()

  # try:
  #   if task["method"] == "POST":
  #     handler_func(*params)
  #   elif task["method"] == "GET":
  #     handler_func(*params)
  #   else:
  #     raise Exception("Unsupported method: %s" % task.method)
  # except Exception as e:
  #   response.status_code = 500
  #   response.data = str(e)
  # else:
  #   response.status_code = 200

  # if response.status_code != 200:
  #   raise Exception("Handler failure: %s. \nTask: %s\nHandler: %s" %
  #           (response.status_code,
  #            task,
  #            handler_func))
  # return handler_func


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
        task_run_counts[handler.__class__] += 1
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
