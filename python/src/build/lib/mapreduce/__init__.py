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

import logging
import os

import pipeline

from . import handlers, status

version = os.environ.get("GAE_VERSION", "").split(".")[0]

if (
    __name__ == "google.appengine.ext.mapreduce"
    and version != "ah-builtin-python-bundle"
):
    msg = (
        "You should not use the mapreduce library that is bundled with the"
        " SDK. You can use the PyPi package at"
        " https://pypi.python.org/pypi/GoogleAppEngineMapReduce or use the "
        "source at https://github.com/GoogleCloudPlatform/appengine-mapreduce "
        "instead."
    )
    logging.warn(msg)

def create_handlers_map(prefix="/mapreduce"):
    """Create new handlers map.

    Args:
      prefix: url prefix to use.

    Returns:
      list of (regexp, handler) pairs for WSGIApplication constructor.
    """
    handlers_map = [
        ("/worker_callback/<path:path>", handlers.MapperWorkerCallbackHandler),
        ("/controller_callback/<path:path>", handlers.ControllerCallbackHandler),
        ("/kickoffjob_callback/<path:path>", handlers.KickOffJobHandler),
        ("/finalizejob_callback/<path:path>", handlers.FinalizeJobHandler),
        ("/command/start_job", handlers.StartJobHandler),
        ("/command/cleanup_job", handlers.CleanUpJobHandler),
        ("/command/abort_job", handlers.AbortJobHandler),
        ("/command/list_configs", status.ListConfigsHandler),
        ("/command/list_jobs", status.ListJobsHandler),
        ("/command/get_job_detail", status.GetJobDetailHandler),
        ("/<path:resource>", status.ResourceHandler),
    ]

    return pipeline.create_handlers_map(f"{prefix}/pipeline") + [
        (f"{prefix}{path}", handler) for path, handler in handlers_map
    ]
