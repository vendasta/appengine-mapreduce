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

"""Model classes which are used to communicate between parts of implementation.

These model classes are describing mapreduce, its current state and
communication messages. They are either stored in the datastore or
serialized to/from json and passed around with other means.
"""

# Disable "Invalid method name"
# pylint: disable=g-bad-name



__all__ = ["MapreduceState",
           "MapperSpec",
           "MapreduceControl",
           "MapreduceSpec",
           "ShardState",
           "CountersMap",
           "TransientShardState",
           "QuerySpec",
           "HugeTask"]

import base64
import datetime
import json
import urllib.error
import urllib.parse
import zlib

from google.cloud import ndb
from pipeline import taskqueue_compat as taskqueue
from mapreduce.compat import datastore_config

from mapreduce import context, hooks, json_util, util

# pylint: disable=protected-access


# Special datastore kinds for MR.
_MAP_REDUCE_KINDS = ("_AE_MR_MapreduceControl",
                     "_AE_MR_MapreduceState",
                     "_AE_MR_ShardState",
                     "_AE_MR_TaskPayload")


class _HugeTaskPayload(ndb.Model):
  """Model object to store task payload."""

  payload = ndb.BlobProperty()

  @classmethod
  def _get_kind(cls):
    return "_AE_MR_TaskPayload"


class HugeTask:
  """HugeTask is a taskqueue.Task-like class that can store big payloads.

  Payloads are stored either in the task payload itself or in the datastore.
  Task handlers should inherit from base_handler.HugeTaskHandler class.
  """

  PAYLOAD_PARAM = "__payload"
  PAYLOAD_KEY_PARAM = "__payload_key"

  # Leave some wiggle room for headers and other fields.
  MAX_TASK_PAYLOAD = 102400 - 1024  # 100KB max push task size minus headroom
  MAX_DB_PAYLOAD = datastore_config.MAX_RPC_BYTES

  PAYLOAD_VERSION_HEADER = "AE-MR-Payload-Version"
  # Update version when payload handling is changed
  # in a backward incompatible way.
  PAYLOAD_VERSION = "1"

  def __init__(self,
               url,
               params,
               name=None,
               eta=None,
               countdown=None,
               parent=None,
               headers=None):
    """Init.

    Args:
      url: task url in str.
      params: a dict from str to str.
      name: task name.
      eta: task eta.
      countdown: task countdown.
      parent: parent entity of huge task's payload.
      headers: a dict of headers for the task.

    Raises:
      ValueError: when payload is too big even for datastore, or parent is
    not specified when payload is stored in datastore.
    """
    self.url = url
    self.name = name
    self.eta = eta
    self.countdown = countdown
    self._headers = {
        "Content-Type": "application/octet-stream",
        self.PAYLOAD_VERSION_HEADER: self.PAYLOAD_VERSION
    }
    if headers:
      self._headers.update(headers)

    # TODO(user): Find a more space efficient way than urlencoding.
    payload_str = urllib.parse.urlencode(params).encode()
    compressed_payload = ""
    if len(payload_str) > self.MAX_TASK_PAYLOAD:
      compressed_payload = zlib.compress(payload_str)

    # Payload is small. Don't bother with anything.
    if not compressed_payload:
      self._payload = payload_str
    else:
      encoded_payload = base64.b64encode(compressed_payload).decode()
      # Compressed payload is small. Don't bother with datastore.
      if len(encoded_payload) < self.MAX_TASK_PAYLOAD:
        self._payload = self.PAYLOAD_PARAM + encoded_payload
      elif len(compressed_payload) > self.MAX_DB_PAYLOAD:
        raise ValueError(
          "Payload from %s to big to be stored in database: %s" %
          (self.name, len(compressed_payload)))
      # Store payload in the datastore.
      else:
        if not parent:
          raise ValueError("Huge tasks should specify parent entity.")

        parent_key = parent.key if hasattr(parent, 'key') else parent
        payload_entity = _HugeTaskPayload(payload=compressed_payload,
                          parent=parent_key)
        payload_key = payload_entity.put()
        self._payload = self.PAYLOAD_KEY_PARAM + payload_key.urlsafe().decode()

  def add(self, queue_name, transactional=False):
    """Add task to the queue."""
    task = self.to_task()
    task.add(queue_name, transactional)

  def to_task(self):
    """Convert to a taskqueue task."""
    # Never pass params to taskqueue.Task. Use payload instead. Otherwise,
    # it's up to a particular taskqueue implementation to generate
    # payload from params. It could blow up payload size over limit.
    return taskqueue.Task(
        url=self.url,
        payload=self._payload,
        name=self.name,
        eta=self.eta,
        countdown=self.countdown,
        headers=self._headers)

  @classmethod
  def decode_payload(cls, request):
    """Decode task payload.

    HugeTask controls its own payload entirely including urlencoding.
    It doesn't depend on any particular web framework.

    Args:
      request: a webapp Request instance.

    Returns:
      A dict of str to str. The same as the params argument to __init__.

    Raises:
      DeprecationWarning: When task payload constructed from an older
        incompatible version of mapreduce.
    """
    # TODO(user): Pass mr_id into headers. Otherwise when payload decoding
    # failed, we can't abort a mr.
    if request.headers.get(cls.PAYLOAD_VERSION_HEADER) != cls.PAYLOAD_VERSION:
      raise DeprecationWarning(
          "Task is generated by an older incompatible version of mapreduce. "
          "Please kill this job manually")
    data = request.get_data().decode()
    return cls._decode_payload(data)

  @classmethod
  def _decode_payload(cls, body):
    compressed_payload_str = None
    if body.startswith(cls.PAYLOAD_KEY_PARAM):
      payload_key_str = body[len(cls.PAYLOAD_KEY_PARAM):]
      payload_key = ndb.Key(urlsafe=payload_key_str)
      payload_entity = payload_key.get()
      compressed_payload_str = payload_entity.payload
    elif body.startswith(cls.PAYLOAD_PARAM):
      compressed_payload_str = body[len(cls.PAYLOAD_PARAM):]
      compressed_payload_str = base64.b64decode(compressed_payload_str)

    if compressed_payload_str:
      payload_str = zlib.decompress(compressed_payload_str).decode()
    else:
      payload_str = body

    result = {}
    for (name, value) in list(urllib.parse.parse_qs(payload_str).items()):
      if len(value) == 1:
        result[name] = value[0]
      else:
        result[name] = value
    return result


class CountersMap(json_util.JsonMixin):
  """Maintains map from counter name to counter value.

  The class is used to provide basic arithmetics of counter values (buil
  add/remove), increment individual values and store/load data from json.
  """

  def __init__(self, initial_map=None):
    """Constructor.

    Args:
      initial_map: initial counter values map from counter name (string) to
        counter value (int).
    """
    if initial_map:
      self.counters = initial_map
    else:
      self.counters = {}

  def __repr__(self):
    """Compute string representation."""
    return "mapreduce.model.CountersMap(%r)" % self.counters

  def get(self, counter_name, default=0):
    """Get current counter value.

    Args:
      counter_name: counter name as string.
      default: default value if one doesn't exist.

    Returns:
      current counter value as int. 0 if counter was not set.
    """
    return self.counters.get(counter_name, default)

  def increment(self, counter_name, delta):
    """Increment counter value.

    Args:
      counter_name: counter name as String.
      delta: increment delta as Integer.

    Returns:
      new counter value.
    """
    current_value = self.counters.get(counter_name, 0)
    new_value = current_value + delta
    self.counters[counter_name] = new_value
    return new_value

  def add_map(self, counters_map):
    """Add all counters from the map.

    For each counter in the passed map, adds its value to the counter in this
    map.

    Args:
      counters_map: CounterMap instance to add.
    """
    for counter_name in counters_map.counters:
      self.increment(counter_name, counters_map.counters[counter_name])

  def sub_map(self, counters_map):
    """Subtracts all counters from the map.

    For each counter in the passed map, subtracts its value to the counter in
    this map.

    Args:
      counters_map: CounterMap instance to subtract.
    """
    for counter_name in counters_map.counters:
      self.increment(counter_name, -counters_map.counters[counter_name])

  def clear(self):
    """Clear all values."""
    self.counters = {}

  def to_json(self):
    """Serializes all the data in this map into json form.

    Returns:
      json-compatible data representation.
    """
    return {"counters": self.counters}

  @classmethod
  def from_json(cls, json):
    """Create new CountersMap from the json data structure, encoded by to_json.

    Args:
      json: json representation of CountersMap .

    Returns:
      an instance of CountersMap with all data deserialized from json.
    """
    counters_map = cls()
    counters_map.counters = dict(json.get("counters", {}))
    return counters_map

  def to_dict(self):
    """Convert to dictionary.

    Returns:
      a dictionary with counter name as key and counter values as value.
    """
    return self.counters


class MapperSpec(json_util.JsonMixin):
  """Contains a specification for the mapper phase of the mapreduce.

  MapperSpec instance can be changed only during mapreduce starting process,
  and it remains immutable for the rest of mapreduce execution. MapperSpec is
  passed as a payload to all mapreduce tasks in JSON encoding as part of
  MapreduceSpec.

  Specifying mapper handlers:
    * '<module_name>.<class_name>' - __call__ method of class instance will be
      called
    * '<module_name>.<function_name>' - function will be called.
    * '<module_name>.<class_name>.<method_name>' - class will be instantiated
      and method called.
  """

  def __init__(self,
               handler_spec,
               input_reader_spec,
               params,
               shard_count,
               output_writer_spec=None):
    """Creates a new MapperSpec.

    Args:
      handler_spec: handler specification as string (see class doc for
        details).
      input_reader_spec: The class name of the input reader to use.
      params: Dictionary of additional parameters for the mapper.
      shard_count: number of shards to process in parallel.

    Properties:
      handler_spec: name of handler class/function to use.
      input_reader_spec: The class name of the input reader to use.
      params: Dictionary of additional parameters for the mapper.
      shard_count: number of shards to process in parallel.
      output_writer_spec: The class name of the output writer to use.
    """
    self.handler_spec = handler_spec
    self.input_reader_spec = input_reader_spec
    self.output_writer_spec = output_writer_spec
    self.shard_count = int(shard_count)
    self.params = params

  def get_handler(self):
    """Get mapper handler instance.

    This always creates a new instance of the handler. If the handler is a
    callable instance, MR only wants to create a new instance at the
    beginning of a shard or shard retry. The pickled callable instance
    should be accessed from TransientShardState.

    Returns:
      handler instance as callable.
    """
    return util.handler_for_name(self.handler_spec)

  handler = property(get_handler)

  def input_reader_class(self):
    """Get input reader class.

    Returns:
      input reader class object.
    """
    return util.for_name(self.input_reader_spec)

  def output_writer_class(self):
    """Get output writer class.

    Returns:
      output writer class object.
    """
    return self.output_writer_spec and util.for_name(self.output_writer_spec)

  def to_json(self):
    """Serializes this MapperSpec into a json-izable object."""
    result = {
        "mapper_handler_spec": self.handler_spec,
        "mapper_input_reader": self.input_reader_spec,
        "mapper_params": self.params,
        "mapper_shard_count": self.shard_count
    }
    if self.output_writer_spec:
      result["mapper_output_writer"] = self.output_writer_spec
    return result

  def __str__(self):
    return "MapperSpec({}, {}, {}, {})".format(
        self.handler_spec, self.input_reader_spec, self.params,
        self.shard_count)

  @classmethod
  def from_json(cls, json):
    """Creates MapperSpec from a dict-like object."""
    return cls(json["mapper_handler_spec"],
               json["mapper_input_reader"],
               json["mapper_params"],
               json["mapper_shard_count"],
               json.get("mapper_output_writer")
              )

  def __eq__(self, other):
    if not isinstance(other, self.__class__):
      return False
    return self.to_json() == other.to_json()


class MapreduceSpec(json_util.JsonMixin):
  """Contains a specification for the whole mapreduce.

  MapreduceSpec instance can be changed only during mapreduce starting process,
  and it remains immutable for the rest of mapreduce execution. MapreduceSpec is
  passed as a payload to all mapreduce tasks in json encoding.
  """

  # Url to call when mapreduce finishes its execution.
  PARAM_DONE_CALLBACK = "done_callback"
  # Queue to use to call done callback
  PARAM_DONE_CALLBACK_QUEUE = "done_callback_queue"

  def __init__(self,
               name,
               mapreduce_id,
               mapper_spec,
               params={},
               hooks_class_name=None):
    """Create new MapreduceSpec.

    Args:
      name: The name of this mapreduce job type.
      mapreduce_id: ID of the mapreduce.
      mapper_spec: JSON-encoded string containing a MapperSpec.
      params: dictionary of additional mapreduce parameters.
      hooks_class_name: The fully qualified name of the hooks class to use.

    Properties:
      name: The name of this mapreduce job type.
      mapreduce_id: unique id of this mapreduce as string.
      mapper: This MapreduceSpec's instance of MapperSpec.
      params: dictionary of additional mapreduce parameters.
      hooks_class_name: The fully qualified name of the hooks class to use.
    """
    self.name = name
    self.mapreduce_id = mapreduce_id
    self.mapper = MapperSpec.from_json(mapper_spec)
    self.params = params
    self.hooks_class_name = hooks_class_name
    self.__hooks = None
    self.get_hooks()  # Fail fast on an invalid hook class.

  def get_hooks(self):
    """Returns a hooks.Hooks class or None if no hooks class has been set."""
    if self.__hooks is None and self.hooks_class_name is not None:
      hooks_class = util.for_name(self.hooks_class_name)
      if not isinstance(hooks_class, type):
        raise ValueError("hooks_class_name must refer to a class, got %s" %
                         type(hooks_class).__name__)
      if not issubclass(hooks_class, hooks.Hooks):
        raise ValueError(
            "hooks_class_name must refer to a hooks.Hooks subclass")
      self.__hooks = hooks_class(self)

    return self.__hooks

  def to_json(self):
    """Serializes all data in this mapreduce spec into json form.

    Returns:
      data in json format.
    """
    mapper_spec = self.mapper.to_json()
    return {
        "name": self.name,
        "mapreduce_id": self.mapreduce_id,
        "mapper_spec": mapper_spec,
        "params": self.params,
        "hooks_class_name": self.hooks_class_name,
    }

  @classmethod
  def from_json(cls, json):
    """Create new MapreduceSpec from the json, encoded by to_json.

    Args:
      json: json representation of MapreduceSpec.

    Returns:
      an instance of MapreduceSpec with all data deserialized from json.
    """
    mapreduce_spec = cls(json["name"],
                         json["mapreduce_id"],
                         json["mapper_spec"],
                         json.get("params"),
                         json.get("hooks_class_name"))
    return mapreduce_spec

  def __str__(self):
    return str(self.to_json())

  def __eq__(self, other):
    if not isinstance(other, self.__class__):
      return False
    return self.to_json() == other.to_json()

  _spec_cache = {}

  @classmethod
  def _get_mapreduce_spec(cls, mr_id):
    """Get Mapreduce spec from mr id."""
    if mr_id in cls._spec_cache:
      return cls.from_json(cls._spec_cache[mr_id])
    state = MapreduceState.get_by_job_id(mr_id)
    spec = state.mapreduce_spec
    spec_json = spec.to_json()
    cls._spec_cache[mr_id] = spec_json
    return spec


class MapreduceState(ndb.Model):
  """Holds accumulated state of mapreduce execution.

  MapreduceState is stored in datastore with a key name equal to the
  mapreduce ID. Only controller tasks can write to MapreduceState.
  """

  RESULT_SUCCESS = "success"
  RESULT_FAILED = "failed"
  RESULT_ABORTED = "aborted"

  _RESULTS = frozenset([RESULT_SUCCESS, RESULT_FAILED, RESULT_ABORTED])

  mapreduce_spec = json_util.JsonProperty(MapreduceSpec)
  active = ndb.BooleanProperty(default=True)
  last_poll_time = ndb.DateTimeProperty()
  counters_map = json_util.JsonProperty(CountersMap, default=CountersMap())
  app_id = ndb.StringProperty(indexed=True)
  writer_state = json_util.JsonProperty(dict)
  active_shards = ndb.IntegerProperty(default=0)
  failed_shards = ndb.IntegerProperty(default=0)
  aborted_shards = ndb.IntegerProperty(default=0)
  result_status = ndb.StringProperty(choices=list(_RESULTS))

  chart_url = ndb.TextProperty(default="")
  chart_width = ndb.IntegerProperty(default=300)
  sparkline_url = ndb.TextProperty(default="")
  start_time = ndb.DateTimeProperty(auto_now_add=True)

  @classmethod
  def _get_kind(cls):
    return "_AE_MR_MapreduceState"

  @classmethod
  def get_key_by_job_id(cls, mapreduce_id):
    return ndb.Key(cls._get_kind(), str(mapreduce_id))

  @classmethod
  def get_by_job_id(cls, mapreduce_id):
    return cls.get_key_by_job_id(mapreduce_id).get()

  def set_processed_counts(self, shards_processed, shards_status):
    pass

  def get_processed(self):
    return self.counters_map.get(context.COUNTER_MAPPER_CALLS)

  processed = property(get_processed)

  @staticmethod
  def create_new(mapreduce_id=None,
                 gettime=datetime.datetime.now):
    if not mapreduce_id:
      mapreduce_id = MapreduceState.new_mapreduce_id()
    state = MapreduceState(id=mapreduce_id,
                           last_poll_time=gettime())
    state.set_processed_counts([], [])
    return state

  @staticmethod
  def new_mapreduce_id():
    return util._get_descending_key()

  def __eq__(self, other):
    if not isinstance(other, self.__class__):
      return False
    return self._properties == other._properties


class TransientShardState:
  """A shard's states that are kept in task payload.

  TransientShardState holds two types of states:
  1. Some states just don't need to be saved to datastore. e.g.
     serialized input reader and output writer instances.
  2. Some states are duplicated from datastore, e.g. slice_id, shard_id.
     These are used to validate the task.
  """

  def __init__(self,
               base_path,
               mapreduce_spec,
               shard_id,
               slice_id,
               input_reader,
               initial_input_reader,
               output_writer=None,
               retries=0,
               handler=None):
    """Init.

    Args:
      base_path: base path of this mapreduce job. Deprecated.
      mapreduce_spec: an instance of MapReduceSpec.
      shard_id: shard id.
      slice_id: slice id. When enqueuing task for the next slice, this number
        is incremented by 1.
      input_reader: input reader instance for this shard.
      initial_input_reader: the input reader instance before any iteration.
        Used by shard retry.
      output_writer: output writer instance for this shard, if exists.
      retries: the number of retries of the current shard. Used to drop
        tasks from old retries.
      handler: map/reduce handler.
    """
    self.base_path = base_path
    self.mapreduce_spec = mapreduce_spec
    self.shard_id = shard_id
    self.slice_id = slice_id
    self.input_reader = input_reader
    self.initial_input_reader = initial_input_reader
    self.output_writer = output_writer
    self.retries = retries
    self.handler = handler
    self._input_reader_json = self.input_reader.to_json()

  def reset_for_retry(self, output_writer):
    """Reset self for shard retry.

    Args:
      output_writer: new output writer that contains new output files.
    """
    self.input_reader = self.initial_input_reader
    self.slice_id = 0
    self.retries += 1
    self.output_writer = output_writer
    self.handler = self.mapreduce_spec.mapper.handler

  def advance_for_next_slice(self, recovery_slice=False):
    """Advance relavent states for next slice.

    Args:
      recovery_slice: True if this slice is running recovery logic.
        See handlers.MapperWorkerCallbackHandler._attempt_slice_recovery
        for more info.
    """
    if recovery_slice:
      self.slice_id += 2
      # Restore input reader to the beginning of the slice.
      self.input_reader = self.input_reader.from_json(self._input_reader_json)
    else:
      self.slice_id += 1

  def to_dict(self):
    """Convert state to dictionary to save in task payload."""
    result = {"mapreduce_spec": self.mapreduce_spec.to_json_str(),
              "shard_id": self.shard_id,
              "slice_id": str(self.slice_id),
              "input_reader_state": self.input_reader.to_json_str(),
              "initial_input_reader_state":
              self.initial_input_reader.to_json_str(),
              "retries": str(self.retries)}
    if self.output_writer:
      result["output_writer_state"] = self.output_writer.to_json_str()
    serialized_handler = util.try_serialize_handler(self.handler)
    if serialized_handler:
      result["serialized_handler"] = serialized_handler
    return result

  @classmethod
  def from_request(cls, request):
    """Create new TransientShardState from webapp request."""
    mapreduce_spec_str = request.get("mapreduce_spec")
    mapreduce_spec = MapreduceSpec.from_json_str(mapreduce_spec_str)
    mapper_spec = mapreduce_spec.mapper
    input_reader_spec_dict = json.loads(request.get("input_reader_state"), cls=json_util.JsonDecoder)
    input_reader = mapper_spec.input_reader_class().from_json(input_reader_spec_dict)
    initial_input_reader_spec_dict = json.loads(request.get("initial_input_reader_state"), cls=json_util.JsonDecoder)
    initial_input_reader = mapper_spec.input_reader_class().from_json(initial_input_reader_spec_dict)

    output_writer = None
    if mapper_spec.output_writer_class():
      output_writer = mapper_spec.output_writer_class().from_json(
          json.loads(request.get("output_writer_state"),
                     cls=json_util.JsonDecoder))
      assert isinstance(output_writer, mapper_spec.output_writer_class()), (
          "{}.from_json returned an instance of wrong class: {}".format(
              mapper_spec.output_writer_class(),
              output_writer.__class__))

    handler = util.try_deserialize_handler(request.get("serialized_handler"))
    if not handler:
      handler = mapreduce_spec.mapper.handler

    return cls(mapreduce_spec.params["base_path"],
               mapreduce_spec,
               str(request.get("shard_id")),
               int(request.get("slice_id")),
               input_reader,
               initial_input_reader,
               output_writer=output_writer,
               retries=int(request.get("retries")),
               handler=handler)


class ShardState(ndb.Model):
  """Single shard execution state."""

  RESULT_SUCCESS = "success"
  RESULT_FAILED = "failed"
  RESULT_ABORTED = "aborted"

  _RESULTS = frozenset([RESULT_SUCCESS, RESULT_FAILED, RESULT_ABORTED])

  _MAX_STATES_IN_MEMORY = 10

  mapreduce_id = ndb.StringProperty(required=True)
  active = ndb.BooleanProperty(default=True)
  input_finished = ndb.BooleanProperty(default=False)
  counters_map = json_util.JsonProperty(CountersMap, default=CountersMap())
  result_status = ndb.StringProperty(choices=list(_RESULTS))
  retries = ndb.IntegerProperty(default=0)
  writer_state = json_util.JsonProperty(dict)
  slice_id = ndb.IntegerProperty(default=0)
  slice_start_time = ndb.DateTimeProperty()
  slice_request_id = ndb.StringProperty()
  slice_retries = ndb.IntegerProperty(default=0)
  acquired_once = ndb.BooleanProperty(default=False)

  update_time = ndb.DateTimeProperty(auto_now=True)
  shard_description = ndb.TextProperty(default="")
  last_work_item = ndb.TextProperty(default="")

  @classmethod
  def _get_kind(cls):
    return "_AE_MR_ShardState"

  def __str__(self):
    kv = {"active": self.active,
          "slice_id": self.slice_id,
          "last_work_item": self.last_work_item,
          "update_time": self.update_time}
    if self.result_status:
      kv["result_status"] = self.result_status
    if self.retries:
      kv["retries"] = self.retries
    if self.slice_start_time:
      kv["slice_start_time"] = self.slice_start_time
    if self.slice_retries:
      kv["slice_retries"] = self.slice_retries
    if self.slice_request_id:
      kv["slice_request_id"] = self.slice_request_id
    if self.acquired_once:
      kv["acquired_once"] = self.acquired_once
    keys = sorted(kv.keys())

    result = "ShardState is {"
    for k in keys:
      result += k + ":" + str(kv[k]) + ","
    result += "}"
    return result

  def reset_for_retry(self):
    self.retries += 1
    self.last_work_item = ""
    self.active = True
    self.result_status = None
    self.input_finished = False
    self.counters_map = CountersMap()
    self.slice_id = 0
    self.slice_start_time = None
    self.slice_request_id = None
    self.slice_retries = 0
    self.acquired_once = False

  def advance_for_next_slice(self, recovery_slice=False):
    self.slice_start_time = None
    self.slice_request_id = None
    self.slice_retries = 0
    self.acquired_once = False
    if recovery_slice:
      self.slice_id += 2
    else:
      self.slice_id += 1

  def set_for_failure(self):
    self.active = False
    self.result_status = self.RESULT_FAILED

  def set_for_abort(self):
    self.active = False
    self.result_status = self.RESULT_ABORTED

  def set_input_finished(self):
    self.input_finished = True

  def is_input_finished(self):
    return self.input_finished

  def set_for_success(self):
    self.active = False
    self.result_status = self.RESULT_SUCCESS
    self.slice_start_time = None
    self.slice_request_id = None
    self.slice_retries = 0
    self.acquired_once = False

  def copy_from(self, other_state):
    for prop_name, prop in self._properties.items():
      setattr(self, prop_name, getattr(other_state, prop_name))

  def __eq__(self, other):
    if not isinstance(other, self.__class__):
      return False
    return self._properties == other._properties

  def get_shard_number(self):
    return int(self.key.string_id().split("-")[-1])

  shard_number = property(get_shard_number)

  def get_shard_id(self):
    return self.key.string_id()

  shard_id = property(get_shard_id)

  @classmethod
  def shard_id_from_number(cls, mapreduce_id, shard_number):
    return "%s-%d" % (mapreduce_id, shard_number)

  @classmethod
  def get_key_by_shard_id(cls, shard_id):
    return ndb.Key(cls._get_kind(), shard_id)

  @classmethod
  def get_by_shard_id(cls, shard_id):
    return cls.get_by_id(shard_id)

  @classmethod
  def find_by_mapreduce_state(cls, mapreduce_state):
    return list(cls.find_all_by_mapreduce_state(mapreduce_state))

  @classmethod
  def find_all_by_mapreduce_state(cls, mapreduce_state):
    keys = cls.calculate_keys_by_mapreduce_state(mapreduce_state)
    use_cache = not ndb.in_transaction()
    i = 0
    while i < len(keys):
      batch = keys[i:i+cls._MAX_STATES_IN_MEMORY]
      if use_cache:
        states = ndb.get_multi(batch)
      else:
        states = ndb.get_multi(batch, use_cache=False, use_global_cache=False)
      for s in states:
        i += 1
        if s is not None:
          yield s

  @classmethod
  def calculate_keys_by_mapreduce_state(cls, mapreduce_state):
    if mapreduce_state is None:
      return []

    keys = []
    for i in range(mapreduce_state.mapreduce_spec.mapper.shard_count):
      shard_id = cls.shard_id_from_number(mapreduce_state.key.string_id(), i)
      keys.append(cls.get_key_by_shard_id(shard_id))
    return keys

  @classmethod
  def create_new(cls, mapreduce_id, shard_number):
    shard_id = cls.shard_id_from_number(mapreduce_id, shard_number)
    state = cls(id=shard_id,
                mapreduce_id=mapreduce_id)
    return state


class MapreduceControl(ndb.Model):
  """Datastore entity used to control mapreduce job execution.

  Only one command may be sent to jobs at a time.

  Properties:
    command: The command to send to the job.
  """

  ABORT = "abort"

  _COMMANDS = frozenset([ABORT])
  _KEY_NAME = "command"

  command = ndb.TextProperty(choices=list(_COMMANDS))

  @classmethod
  def _get_kind(cls):
    return "_AE_MR_MapreduceControl"

  @classmethod
  def get_key_by_job_id(cls, mapreduce_id):
    """Retrieves the Key for a mapreduce ID.

    Args:
      mapreduce_id: The job to fetch.

    Returns:
      Datastore Key for the command for the given job ID.
    """
    return ndb.Key(cls._get_kind(), "{}:{}".format(mapreduce_id, cls._KEY_NAME))

  @classmethod
  def abort(cls, mapreduce_id, **kwargs):
    """Causes a job to abort.

    Args:
      mapreduce_id: The job to abort. Not verified as a valid job.
    """
    cls(id="{}:{}".format(mapreduce_id, cls._KEY_NAME),
        command=cls.ABORT).put()


class QuerySpec:
  """Encapsulates everything about a query needed by DatastoreInputReader."""

  DEFAULT_BATCH_SIZE = 50
  DEFAULT_OVERSPLIT_FACTOR = 1

  def __init__(self,
               entity_kind,
               keys_only=None,
               filters=None,
               batch_size=None,
               oversplit_factor=None,
               model_class_path=None,
               app=None,
               ns=None):
    self.entity_kind = entity_kind
    self.keys_only = keys_only or False
    self.filters = filters or None
    self.batch_size = batch_size or self.DEFAULT_BATCH_SIZE
    self.oversplit_factor = (oversplit_factor or
                             self.DEFAULT_OVERSPLIT_FACTOR)
    self.model_class_path = model_class_path
    self.app = app
    self.ns = ns

  def to_json(self):
    return {"entity_kind": self.entity_kind,
            "keys_only": self.keys_only,
            "filters": self.filters,
            "batch_size": self.batch_size,
            "oversplit_factor": self.oversplit_factor,
            "model_class_path": self.model_class_path,
            "app": self.app,
            "ns": self.ns}

  @classmethod
  def from_json(cls, json):
    return cls(json["entity_kind"],
               json["keys_only"],
               json["filters"],
               json["batch_size"],
               json["oversplit_factor"],
               json["model_class_path"],
               json["app"],
               json["ns"])
