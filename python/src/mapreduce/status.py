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

"""Status page handler for mapreduce framework."""

__author__ = ("aizatsky@google.com (Mike Aizatsky)",
              "bslatkin@google.com (Brett Slatkin)")

import os
import pkgutil
import re
import time
import zipfile

import yaml
from flask import Response, request
from flask.views import MethodView
from google.cloud import ndb
from mapreduce import base_handler
from mapreduce import errors
from mapreduce import model


# TODO(user): a list of features we'd like to have in status page:
# - show sparklet of entities/sec on index page
# - shard bar chart should color finished shards differently

# mapreduce.yaml file names
MR_YAML_NAMES = ["mapreduce.yaml", "mapreduce.yml"]


class BadStatusParameterError(Exception):
  """A parameter passed to a status handler was invalid."""


class UserParam:
  def __init__(self, name, default=None, value=None):
    self.name = name
    self.default = default
    self.value = value


class MapperInfo:
  def __init__(self, handler, input_reader, output_writer=None,
               params=None, params_validator=None):
    self.handler = handler
    self.input_reader = input_reader
    self.output_writer = output_writer
    self.params = params
    self.params_validator = params_validator


class MapreduceInfo:
  def __init__(self, name, mapper, params=None, params_validator=None):
    self.name = name
    self.mapper = mapper
    self.params = params
    self.params_validator = params_validator


class MapReduceYaml:
  """Parsed representation of mapreduce.yaml."""

  def __init__(self, mapreduce=None):
    self.mapreduce = mapreduce or []

  @staticmethod
  def to_dict(mapreduce_yaml):
    all_configs = []
    for config in mapreduce_yaml.mapreduce:
      out = {
          "name": config.name,
          "mapper_input_reader": config.mapper.input_reader,
          "mapper_handler": config.mapper.handler,
      }
      if config.mapper.params_validator:
        out["mapper_params_validator"] = config.mapper.params_validator
      if config.mapper.params:
        param_defaults = {}
        for param in config.mapper.params:
          param_defaults[param.name] = param.default or param.value
        out["mapper_params"] = param_defaults
      if config.params:
        param_defaults = {}
        for param in config.params:
          param_defaults[param.name] = param.default or param.value
        out["params"] = param_defaults
      if config.mapper.output_writer:
        out["mapper_output_writer"] = config.mapper.output_writer
      all_configs.append(out)

    return all_configs


# N.B. Sadly, we currently don't have and ability to determine
# application root dir at run time. We need to walk up the directory structure
# to find it.
def find_mapreduce_yaml(status_file=__file__):
  """Traverse directory trees to find mapreduce.yaml file.

  Begins with the location of status.py and then moves on to check the working
  directory.

  Args:
    status_file: location of status.py, overridable for testing purposes.

  Returns:
    the path of mapreduce.yaml file or None if not found.
  """
  checked = set()
  yaml = _find_mapreduce_yaml(os.path.dirname(status_file), checked)
  if not yaml:
    yaml = _find_mapreduce_yaml(os.getcwd(), checked)
  return yaml


def _find_mapreduce_yaml(start, checked):
  """Traverse the directory tree identified by start until a directory already
  in checked is encountered or the path of mapreduce.yaml is found.

  Checked is present both to make loop termination easy to reason about and so
  that the same directories do not get rechecked.

  Args:
    start: the path to start in and work upward from
    checked: the set of already examined directories

  Returns:
    the path of mapreduce.yaml file or None if not found.
  """
  dir = start
  while dir not in checked:
    checked.add(dir)
    for mr_yaml_name in MR_YAML_NAMES:
      yaml_path = os.path.join(dir, mr_yaml_name)
      if os.path.exists(yaml_path):
        return yaml_path
    dir = os.path.dirname(dir)
  return None


def parse_mapreduce_yaml(contents):
  """Parses mapreduce.yaml file contents."""
  try:
    docs = list(yaml.safe_load_all(contents))
  except yaml.YAMLError as e:
    raise errors.BadYamlError(e)

  if not docs:
    raise errors.BadYamlError("No configs found in mapreduce.yaml")
  if len(docs) > 1:
    raise errors.MultipleDocumentsInMrYaml("Found %d YAML documents" %
                                           len(docs))

  doc = docs[0]
  if not doc or "mapreduce" not in doc:
    raise errors.BadYamlError("No configs found in mapreduce.yaml")

  mr_configs = []
  for entry in doc["mapreduce"]:
    if "name" not in entry or "mapper" not in entry:
      raise errors.BadYamlError("Each mapreduce entry needs 'name' and 'mapper'")

    mapper_dict = entry["mapper"]
    if not isinstance(mapper_dict, dict):
      raise errors.BadYamlError("'mapper' must be a mapping")
    if "handler" not in mapper_dict:
      raise errors.BadYamlError("Missing required field 'handler' in mapper")
    if "input_reader" not in mapper_dict:
      raise errors.BadYamlError("Missing required field 'input_reader' in mapper")

    mapper_params = None
    if "params" in mapper_dict:
      mapper_params = []
      for p in mapper_dict["params"]:
        name = p.get("name", "")
        if not re.match(r'^[a-zA-Z0-9_\.]+$', name):
          raise errors.BadYamlError("Invalid param name: %s" % name)
        mapper_params.append(UserParam(name, p.get("default"), p.get("value")))

    mapper = MapperInfo(
        handler=mapper_dict["handler"],
        input_reader=mapper_dict["input_reader"],
        output_writer=mapper_dict.get("output_writer"),
        params=mapper_params,
        params_validator=mapper_dict.get("params_validator"))

    entry_params = None
    if "params" in entry:
      entry_params = [UserParam(p["name"], p.get("default"), p.get("value"))
                      for p in entry["params"]]

    mr_configs.append(MapreduceInfo(
        name=entry["name"],
        mapper=mapper,
        params=entry_params,
        params_validator=entry.get("params_validator")))

  job_names = {j.name for j in mr_configs}
  if len(mr_configs) != len(job_names):
    raise errors.BadYamlError(
        "Overlapping mapreduce names; names must be unique")

  return MapReduceYaml(mapreduce=mr_configs)


def get_mapreduce_yaml(parse=parse_mapreduce_yaml):
  """Locates mapreduce.yaml, loads and parses its info.

  Args:
    parse: Used for testing.

  Returns:
    MapReduceYaml object.

  Raises:
    errors.BadYamlError: when contents is not a valid mapreduce.yaml file or the
    file is missing.
  """
  mr_yaml_path = find_mapreduce_yaml()
  if not mr_yaml_path:
    raise errors.MissingYamlError()
  mr_yaml_file = open(mr_yaml_path)
  try:
    return parse(mr_yaml_file.read())
  finally:
    mr_yaml_file.close()


class ResourceHandler(MethodView):
    """Handler for static resources."""

    _RESOURCE_MAP = {
        "status": ("overview.html", "text/html"),
        "detail": ("detail.html", "text/html"),
        "base.css": ("base.css", "text/css"),
        "jquery.js": ("jquery-1.6.1.min.js", "text/javascript"),
        "jquery-json.js": ("jquery.json-2.2.min.js", "text/javascript"),
        "jquery-url.js": ("jquery.url.js", "text/javascript"),
        "status.js": ("status.js", "text/javascript"),
    }

    def get(self, resource):
        if resource not in self._RESOURCE_MAP:
            return Response("Resource not found.", status=404)

        real_path, content_type = self._RESOURCE_MAP[resource]
        path = os.path.join(os.path.dirname(__file__), "static", real_path)

        # It's possible we're inside a zipfile (zipimport).  If so, path
        # will include 'something.zip'.
        if ('.zip' + os.sep) in path:
            (zip_file, zip_path) = os.path.relpath(path).split('.zip' + os.sep, 1)
            content = zipfile.ZipFile(zip_file + '.zip').read(zip_path)
        else:
            try:
                data = pkgutil.get_data(__name__, "static/" + real_path)
            except AttributeError:  # Python < 2.6.
                data = None
            content = data or open(path, 'rb').read()

        response = Response(content)
        response.headers["Cache-Control"] = "public; max-age=300"
        response.headers["Content-Type"] = content_type

        return response


class ListConfigsHandler(base_handler.GetJsonHandler):
  """Lists mapreduce configs as JSON for users to start jobs."""

  def handle(self):
    self.json_response["configs"] = MapReduceYaml.to_dict(get_mapreduce_yaml())


class ListJobsHandler(base_handler.GetJsonHandler):
  """Lists running and completed mapreduce jobs for an overview as JSON."""

  def handle(self):
    cursor = request.args.get("cursor")
    count = int(request.args.get("count", "50"))

    query = model.MapreduceState.query()
    if cursor:
      query = query.filter(
          model.MapreduceState.key >= ndb.Key(urlsafe=cursor))
    query = query.order(model.MapreduceState.key)

    jobs_list = query.fetch(count + 1)
    if len(jobs_list) == (count + 1):
      self.json_response["cursor"] = jobs_list[-1].key.urlsafe().decode()
      jobs_list = jobs_list[:-1]

    all_jobs = []
    for job in jobs_list:
      out = {
          # Data shared between overview and detail pages.
          "name": job.mapreduce_spec.name,
          "mapreduce_id": job.mapreduce_spec.mapreduce_id,
          "active": job.active,
          "start_timestamp_ms":
              int(time.mktime(job.start_time.utctimetuple()) * 1000),
          "updated_timestamp_ms":
              int(time.mktime(job.last_poll_time.utctimetuple()) * 1000),

          # Specific to overview page.
          "chart_url": job.sparkline_url,
          "chart_width": job.chart_width,
          "active_shards": job.active_shards,
          "shards": job.mapreduce_spec.mapper.shard_count,
      }
      if job.result_status:
        out["result_status"] = job.result_status
      all_jobs.append(out)

    self.json_response["jobs"] = all_jobs


class GetJobDetailHandler(base_handler.GetJsonHandler):
  """Retrieves the details of a mapreduce job as JSON."""

  def handle(self):
    mapreduce_id = request.args.get("mapreduce_id")
    if not mapreduce_id:
      raise BadStatusParameterError("'mapreduce_id' was invalid")
    job = model.MapreduceState.get_by_job_id(mapreduce_id)
    if job is None:
      raise KeyError("Could not find job with ID %r" % mapreduce_id)

    self.json_response.update(job.mapreduce_spec.to_json())
    self.json_response.update(job.counters_map.to_json())
    self.json_response.update({
        # Shared with overview page.
        "active": job.active,
        "start_timestamp_ms":
            int(time.mktime(job.start_time.utctimetuple()) * 1000),
        "updated_timestamp_ms":
            int(time.mktime(job.last_poll_time.utctimetuple()) * 1000),

        # Specific to detail page.
        "chart_url": job.chart_url,
        "chart_width": job.chart_width,
    })
    self.json_response["result_status"] = job.result_status

    all_shards = []
    for shard in model.ShardState.find_all_by_mapreduce_state(job):
      out = {
          "active": shard.active,
          "result_status": shard.result_status,
          "shard_number": shard.shard_number,
          "shard_id": shard.shard_id,
          "updated_timestamp_ms":
              int(time.mktime(shard.update_time.utctimetuple()) * 1000),
          "shard_description": shard.shard_description,
          "last_work_item": shard.last_work_item,
      }
      out.update(shard.counters_map.to_json())
      all_shards.append(out)
    all_shards.sort(key=lambda x: x["shard_number"])
    self.json_response["shards"] = all_shards
