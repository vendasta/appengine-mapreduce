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

import unittest

from flask import Flask
import werkzeug

from mapreduce import base_handler
from mapreduce import errors
from mapreduce import parameters
from mapreduce import util


class TaskQueueHandlerTest(unittest.TestCase):
    """Tests for TaskQueueHandler."""

    def setUp(self):
        self.handler = base_handler.TaskQueueHandler()

    def testPostNoTaskQueueHeader(self):
        """Test calling post() without valid taskqueue header."""
        app = Flask(__name__)
        with app.test_request_context(
            headers={
                "X-AppEngine-TaskName": "task_name",
            }
        ):
          with self.assertRaises(werkzeug.exceptions.Forbidden):
              self.handler.dispatch_request()

    def testTaskRetryCount(self):
        app = Flask(__name__)
        with app.test_request_context(
            method="POST",
            headers={
                "X-AppEngine-QueueName": "default",
                "X-AppEngine-TaskName": "task_name",
            }
        ):
            with self.assertRaises(NotImplementedError):
                self.handler.handle()
            self.assertEqual(0, self.handler.task_retry_count())

        with app.test_request_context(
            method="POST",
            headers={
                "X-AppEngine-QueueName": "default",
                "X-AppEngine-TaskName": "task_name",
                "X-AppEngine-TaskExecutionCount": 5,
            }
        ):
            with self.assertRaises(NotImplementedError):
                self.handler.handle()
            self.assertEqual(5, self.handler.task_retry_count())


class FaultyTaskQueueHandler(base_handler.TaskQueueHandler):
    """A handler that always fails at _preprocess."""

    dropped = False
    handled = False

    def _preprocess(self):
        raise Exception()

    def _drop_gracefully(self):
        self.dropped = True

    def handle(self):
        self.handled = True

    @classmethod
    def reset(cls):
        cls.dropped = False
        cls.handled = False


class FaultyTaskQueueHandlerTest(unittest.TestCase):

    def setUp(self):
        FaultyTaskQueueHandler.reset()
        self.handler = FaultyTaskQueueHandler()

    def testSmoke(self):
        app = Flask(__name__)
        with app.test_request_context(
            headers={
                "X-AppEngine-QueueName": "default",
                "X-AppEngine-TaskName": "task_name",
                util._MR_ID_TASK_HEADER: "mr_id",
            }
        ):
            response = self.handler.dispatch_request()
            self.assertTrue(self.handler.dropped)
            self.assertFalse(self.handler.handled)
            self.assertEqual(200, response.status_code)

    def testTaskRetriedTooManyTimes(self):
        app = Flask(__name__)
        with app.test_request_context(
            headers={
                "X-AppEngine-QueueName": "default",
                "X-AppEngine-TaskName": "task_name",
                "X-AppEngine-TaskExecutionCount": parameters.config.TASK_MAX_ATTEMPTS,
            }
        ):
            response = self.handler.dispatch_request()
            self.assertTrue(self.handler.dropped)
            self.assertFalse(self.handler.handled)
            self.assertEqual(200, response.status_code)


class JsonErrorHandler(base_handler.JsonHandler):
    """JsonHandler that raises an error when invoked."""

    # pylint: disable=super-init-not-called
    def __init__(self, error):
        """Constructor.

        Args:
          error: The error to raise when handle() is called.
        """
        self.error = error
        self.json_response = {}

    def handle(self):
        """Raise an error."""
        raise self.error


class JsonHandlerTest(unittest.TestCase):
    """Tests for JsonHandler."""

    def setUp(self):
        self.handler = base_handler.JsonHandler()

    def testBasePath(self):
        """Test base_path calculation."""
        app = Flask(__name__)
        with app.test_request_context("/mapreduce_base/start"):
            with self.assertRaises(base_handler.BadRequestPathError):
                self.handler.base_path()

        with app.test_request_context("/mapreduce_base/command/start"):
            self.assertEqual("/mapreduce_base", self.handler.base_path())

        with app.test_request_context("/command/start"):
            self.assertEqual("", self.handler.base_path())

        with app.test_request_context("/map/reduce/base/command/start"):
            self.assertEqual("/map/reduce/base", self.handler.base_path())

    def testMissingYamlError(self):
        """Test that this error sets the expected response fields."""
        handler = JsonErrorHandler(errors.MissingYamlError)

        app = Flask(__name__)
        with app.test_request_context(
            headers={
                "X-Requested-With": "XMLHttpRequest",
            }
        ):
            handler._handle_wrapper()
            self.assertEqual("Notice", handler.json_response["error_class"])
            self.assertEqual(
                "Could not find 'mapreduce.yaml'", handler.json_response["error_message"]
            )

    def testError(self):
        """Test that an error sets the expected response fields."""
        handler = JsonErrorHandler(Exception("bill hicks"))

        app = Flask(__name__)
        with app.test_request_context(
            headers={
                "X-Requested-With": "XMLHttpRequest",
            }
        ):
            handler._handle_wrapper()
            self.assertEqual("Exception", handler.json_response["error_class"])
            self.assertEqual("bill hicks", handler.json_response["error_message"])


if __name__ == "__main__":
    unittest.main()
