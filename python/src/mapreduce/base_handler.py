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

"""Base handler class for all mapreduce handlers."""



# pylint: disable=protected-access
# pylint: disable=g-bad-name
# pylint: disable=g-import-not-at-top

import importlib
import logging
import pkgutil
import json

pipeline_base = None

if pkgutil.find_loader('mapreduce.pipeline_base') is not None:
  pipeline_base = importlib.import_module('mapreduce.pipeline_base')

from flask import abort, make_response, request
from flask.views import MethodView
from mapreduce import errors
from mapreduce import json_util
from mapreduce import model
from mapreduce import parameters


class Error(Exception):
  """Base-class for exceptions in this module."""


class BadRequestPathError(Error):
  """The request path for the handler is invalid."""


class TaskQueueHandler(MethodView):
  """Base class for handlers intended to be run only from the task queue.

  Sub-classes should implement
  1. the 'handle' method for all POST request.
  2. '_preprocess' method for decoding or validations before handle.
  3. '_drop_gracefully' method if _preprocess fails and the task has to
     be dropped.

  """

  _DEFAULT_USER_AGENT = "App Engine Python MR"

  def __init__(self, *args, **kwargs):
      self._preprocess_success = False
      super().__init__(*args, **kwargs)

  def dispatch_request(self, *args, **kwargs):
    # Check request is from taskqueue.
      if "X-AppEngine-QueueName" not in request.headers:
          logging.error(request.headers)
          logging.error("Task queue handler received non-task queue request")
          abort(403, "Task queue handler received non-task queue request")

    # Check task has not been retried too many times.
      if self.task_retry_count() + 1 > parameters.config.TASK_MAX_ATTEMPTS:
          logging.error(
              "Task %s has been attempted %s times. Dropping it permanently.",
              request.headers["X-AppEngine-TaskName"],
              self.task_retry_count() + 1)
          self._drop_gracefully()
          return make_response("Task dropped", 200)

      try:
          self._preprocess()
          self._preprocess_success = True
      except Exception:
          self._preprocess_success = False
          logging.exception(
              "Preprocess task %s failed. Dropping it permanently.",
              request.headers["X-AppEngine-TaskName"])
          self._drop_gracefully()
          return make_response("Preprocessing failed", 200)

      return super().dispatch_request()

  def post(self):
    if self._preprocess_success:
      self.handle()
      return make_response("Task completed", 200)
    return make_response("Preprocessing failed", 200)

  def handle(self):
    """To be implemented by subclasses."""
    raise NotImplementedError()

  class _RequestWrapper:
    """Container of a request and associated parameters."""

    def __init__(self, request):
      self._request = request

    def get(self, name, default=""):
      return self._request.form.get(name, self._request.args.get(name, default))
    
    def __getattr__(self, name):
      if name == "get":
        return self.get
      if name == "_request":
        return self._request
      return self._request.__getattr__(name)

  def _preprocess(self):
    """Preprocess.

    This method is called after webapp initialization code has been run
    successfully. It can thus access self.request, self.response and so on.
    """
    self.request = self._RequestWrapper(request)

  def _drop_gracefully(self):
    """Drop task gracefully.

    When preprocess failed, this method is called before the task is dropped.
    """
    pass

  def task_retry_count(self):
    """Number of times this task has been retried."""
    return int(request.headers.get("X-AppEngine-TaskExecutionCount", 0))

  def retry_task(self):
    """Ask taskqueue to retry this task.

    Even though raising an exception can cause a task retry, it
    will flood logs with highly visible ERROR logs. Handlers should uses
    this method to perform controlled task retries. Only raise exceptions
    for those deserve ERROR log entries.
    """
    abort(503, "Retry Task")


class JsonHandler(MethodView):
  """Base class for JSON handlers for user interface.

  Sub-classes should implement the 'handle' method. They should put their
  response data in the 'self.json_response' dictionary. Any exceptions raised
  by the sub-class implementation will be sent in a JSON response with the
  name of the error_class and the error_message.
  """

  def __init__(self, *args):
    """Initializer."""
    super().__init__(*args)
    self.json_response = {}

  def base_path(self):
    """Base path for all mapreduce-related urls.

    JSON handlers are mapped to /base_path/command/command_name thus they
    require special treatment.

    Raises:
      BadRequestPathError: if the path does not end with "/command".

    Returns:
      The base path.
    """
    path = request.path
    base_path = path[:path.rfind("/")]
    if not base_path.endswith("/command"):
      raise BadRequestPathError(
          "Json handlers should have /command path prefix")
    return base_path[:base_path.rfind("/")]

  def _handle_wrapper(self):
    """The helper method for handling JSON Post and Get requests."""
    if request.headers.get("X-Requested-With") != "XMLHttpRequest":
      logging.error("Got JSON request with no X-Requested-With header")
      abort(403, "Got JSON request with no X-Requested-With header")

    self.json_response.clear()
    try:
      self.handle()
    except errors.MissingYamlError:
      logging.debug("Could not find 'mapreduce.yaml' file.")
      self.json_response.clear()
      self.json_response["error_class"] = "Notice"
      self.json_response["error_message"] = "Could not find 'mapreduce.yaml'"
    except Exception as e:
      logging.exception("Error in JsonHandler, returning exception.")
      # TODO(user): Include full traceback here for the end-user.
      self.json_response.clear()
      self.json_response["error_class"] = e.__class__.__name__
      self.json_response["error_message"] = str(e)

    try:
      response = make_response(json.dumps(self.json_response, cls=json_util.JsonEncoder), 200)
      response.headers["Content-Type"] = "text/javascript"
      return response
    # pylint: disable=broad-except
    except Exception as e:
      logging.exception("Could not serialize to JSON")
      abort(500, "Could not serialize to JSON")

  def handle(self):
    """To be implemented by sub-classes."""
    raise NotImplementedError()


class PostJsonHandler(JsonHandler):
  """JSON handler that accepts POST requests."""

  def post(self):
    return self._handle_wrapper()


class GetJsonHandler(JsonHandler):
  """JSON handler that accepts GET posts."""

  def get(self):
    return self._handle_wrapper()


class HugeTaskHandler(TaskQueueHandler):
  """Base handler for processing HugeTasks."""

  class _RequestWrapper:
    """Container of a request and associated parameters."""

    def __init__(self, request):
      self._request = request
      self._params = model.HugeTask.decode_payload(request)

    def get(self, name, default=""):
      return self._params.get(name, default)

    def set(self, name, value):
      self._params[name] = value

    def __getattr__(self, name):
      return getattr(self._request, name)

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  def _preprocess(self):
    self.request = self._RequestWrapper(request)


if pipeline_base:
  # For backward compatiblity.
  PipelineBase = pipeline_base.PipelineBase
else:
  PipelineBase = None
