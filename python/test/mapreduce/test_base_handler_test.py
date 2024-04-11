import unittest
from flask import Flask
from your_module import YourHandler

class YourHandlerTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.handler = YourHandler()

    def testTaskRetryCount(self):
        with self.app.test_request_context(
            headers={
                "X-AppEngine-QueueName": "default",
                "X-AppEngine-TaskName": "task_name",
            }
        ):
            self.handler.dispatch_request()
            self.assertEqual(0, self.handler.task_retry_count())

        with self.app.test_request_context(
            method="POST",
            headers={
                "X-AppEngine-QueueName": "default",
                "X-AppEngine-TaskName": "task_name",
                "X-AppEngine-TaskExecutionCount": 5,
            }
        ):
            self.handler.dispatch_request()
            self.assertEqual(5, self.handler.task_retry_count())

if __name__ == "__main__":
    unittest.main()