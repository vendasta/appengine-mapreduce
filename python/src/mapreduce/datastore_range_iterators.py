#!/usr/bin/env python
# Copyright 2015 Google Inc. All Rights Reserved.
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
"""Helpers iterators for input_readers.DatastoreInputReader."""



# pylint: disable=g-bad-name
import itertools
from google.cloud import ndb
from mapreduce.compat import key_range
from mapreduce import json_util
from mapreduce import key_ranges
from mapreduce import model
from mapreduce import namespace_range
from mapreduce import property_range
from mapreduce import util

Cursor = ndb.Cursor

__all__ = [
    "RangeIteratorFactory",
    "RangeIterator",
    "AbstractKeyRangeIterator",
    "KeyRangeModelIterator",
    "KeyRangeEntityIterator",
    "KeyRangeKeyIterator",
    "KeyRangeEntityProtoIterator",
    "Cursor",]


class RangeIteratorFactory:
  """Factory to create RangeIterator."""

  @classmethod
  def create_property_range_iterator(cls,
                                     p_range,
                                     ns_range,
                                     query_spec):
    return _PropertyRangeModelIterator(p_range,
                                       ns_range,
                                       query_spec)

  @classmethod
  def create_multi_property_range_iterator(cls,
                                           p_range_iters):
    return _MultiPropertyRangeModelIterator(p_range_iters)

  @classmethod
  def create_key_ranges_iterator(cls,
                                 k_ranges,
                                 query_spec,
                                 key_range_iter_cls):
    return _KeyRangesIterator(k_ranges, query_spec, key_range_iter_cls)

  @classmethod
  def from_json(cls, json):
    return _RANGE_ITERATORS[json["name"]].from_json(json)


class RangeIterator(json_util.JsonMixin):
  """Interface for DatastoreInputReader helpers."""

  def __iter__(self):
    raise NotImplementedError()

  def __repr__(self):
    raise NotImplementedError()

  def to_json(self):
    raise NotImplementedError()

  @classmethod
  def from_json(cls, json):
    raise NotImplementedError()


class _PropertyRangeModelIterator(RangeIterator):
  """Yields ndb model entities within a property range."""

  def __init__(self, p_range, ns_range, query_spec):
    self._property_range = p_range
    self._ns_range = ns_range
    self._query_spec = query_spec
    self._cursor = None
    self._query = None

  def __repr__(self):
    return "PropertyRangeIterator for %s" % str(self._property_range)

  def __iter__(self):
    for ns in self._ns_range:
      self._query = self._property_range.make_query(ns)
      self._query = self._query.iter(keys_only=self._query_spec.keys_only,
                                     start_cursor=self._cursor)
      yield from self._query
      self._query = None
      self._cursor = None
      if ns != self._ns_range.namespace_end:
        self._ns_range = self._ns_range.with_start_after(ns)

  def to_json(self):
    cursor = self._cursor
    cursor_object = False
    if self._query is not None:
      cursor_object = True
      cursor = self._query.cursor_after().to_websafe_string().decode()

    if isinstance(cursor, ndb.Cursor):
      cursor_object = True
      cursor = cursor.to_websafe_string().decode()

    return {"property_range": self._property_range.to_json(),
            "query_spec": self._query_spec.to_json(),
            "cursor": cursor,
            "ns_range": self._ns_range.to_json_object(),
            "name": self.__class__.__name__,
            "cursor_object": cursor_object}

  @classmethod
  def from_json(cls, json):
    obj = cls(property_range.PropertyRange.from_json(json["property_range"]),
              namespace_range.NamespaceRange.from_json_object(json["ns_range"]),
              model.QuerySpec.from_json(json["query_spec"]))
    cursor = json["cursor"]
    if cursor and json["cursor_object"]:
      obj._cursor = ndb.Cursor(urlsafe=cursor)
    else:
      obj._cursor = cursor
    return obj


class _MultiPropertyRangeModelIterator(RangeIterator):
  """Yields ndb model entities within a list of disjoint property ranges."""

  def __init__(self, p_range_iters):
    self._iters = p_range_iters

  def __repr__(self):
    return "MultiPropertyRangeIterator combining %s" % str(
      [str(it) for it in self._iters])

  def __iter__(self):
    yield from itertools.chain.from_iterable(self._iters)

  def to_json(self):
    json = {"name": self.__class__.__name__,
            "num_ranges": len(self._iters)}

    for i in range(len(self._iters)):
      json_item = self._iters[i].to_json()
      query_spec = json_item["query_spec"]
      item_name = json_item["name"]
      del json_item["query_spec"]
      del json_item["name"]
      json[str(i)] = json_item
    json["query_spec"] = query_spec
    json["item_name"] = item_name

    return json

  @classmethod
  def from_json(cls, json):
    num_ranges = int(json["num_ranges"])
    query_spec = json["query_spec"]
    item_name = json["item_name"]

    p_range_iters = []
    for i in range(num_ranges):
      json_item = json[str(i)]
      json_item["query_spec"] = query_spec
      json_item["name"] = item_name
      p_range_iters.append(_PropertyRangeModelIterator.from_json(json_item))

    obj = cls(p_range_iters)
    return obj


class _KeyRangesIterator(RangeIterator):
  """Create an iterator over a key_ranges.KeyRanges object."""

  def __init__(self,
               k_ranges,
               query_spec,
               key_range_iter_cls):
    self._key_ranges = k_ranges
    self._query_spec = query_spec
    self._key_range_iter_cls = key_range_iter_cls
    self._current_iter = None
    self._current_key_range = None

  def __repr__(self):
    return "KeyRangesIterator for %s" % str(self._key_ranges)

  def __iter__(self):
    while True:
      if self._current_iter:
        yield from self._current_iter

      try:
        k_range = next(self._key_ranges)
        self._current_iter = self._key_range_iter_cls(k_range,
                                                      self._query_spec)
      except StopIteration:
        self._current_iter = None
        break

  def to_json(self):
    current_iter = None
    if self._current_iter:
      current_iter = self._current_iter.to_json()

    return {"key_ranges": self._key_ranges.to_json(),
            "query_spec": self._query_spec.to_json(),
            "current_iter": current_iter,
            "key_range_iter_cls": self._key_range_iter_cls.__name__,
            "name": self.__class__.__name__}

  @classmethod
  def from_json(cls, json):
    key_range_iter_cls = _KEY_RANGE_ITERATORS[json["key_range_iter_cls"]]
    obj = cls(key_ranges.KeyRangesFactory.from_json(json["key_ranges"]),
              model.QuerySpec.from_json(json["query_spec"]),
              key_range_iter_cls)

    current_iter = None
    if json["current_iter"]:
      current_iter = key_range_iter_cls.from_json(json["current_iter"])
    # pylint: disable=protected-access
    obj._current_iter = current_iter
    return obj


_RANGE_ITERATORS = {
    _PropertyRangeModelIterator.__name__: _PropertyRangeModelIterator,
    _MultiPropertyRangeModelIterator.__name__: _MultiPropertyRangeModelIterator,
    _KeyRangesIterator.__name__: _KeyRangesIterator
    }


class AbstractKeyRangeIterator(json_util.JsonMixin):
  """Iterates over a single KeyRange and yields value for each key."""

  def __init__(self, k_range, query_spec):
    self._key_range = k_range
    self._query_spec = query_spec
    self._cursor = None
    self._query = None

  def __iter__(self):
    raise NotImplementedError()

  def _get_cursor(self):
    raise NotImplementedError()

  def to_json(self):
    cursor = self._get_cursor()
    cursor_object = False
    if cursor and isinstance(cursor, ndb.Cursor):
      cursor = cursor.to_websafe_string()
      cursor_object = True
    return {"key_range": self._key_range.to_json(),
            "query_spec": self._query_spec.to_json(),
            "cursor": cursor,
            "cursor_object": cursor_object}

  @classmethod
  def from_json(cls, json):
    obj = cls(key_range.KeyRange.from_json(json["key_range"]),
              model.QuerySpec.from_json(json["query_spec"]))
    cursor = json["cursor"]
    if cursor and json["cursor_object"]:
      obj._cursor = ndb.Cursor(urlsafe=cursor)
    else:
      obj._cursor = cursor
    return obj


class KeyRangeModelIterator(AbstractKeyRangeIterator):
  """Yields ndb model entities with a key range."""

  def __iter__(self):
    self._query = self._key_range.make_ascending_query(
        util.for_name(self._query_spec.model_class_path),
        filters=self._query_spec.filters)

    self._query = self._query.iter(keys_only=self._query_spec.keys_only,
                                   start_cursor=self._cursor)
    yield from self._query

  def _get_cursor(self):
    if self._query is None:
      return self._cursor
    return self._query.cursor_after()


class KeyRangeEntityIterator(AbstractKeyRangeIterator):
  """Yields ndb entities within a key range."""

  _KEYS_ONLY = False

  def __iter__(self):
    self._query = self._key_range.make_ascending_datastore_query(
        self._query_spec.entity_kind, filters=self._query_spec.filters)
    self._query = self._query.iter(
        keys_only=self._query_spec.keys_only or self._KEYS_ONLY,
        start_cursor=self._cursor)
    yield from self._query

  def _get_cursor(self):
    if self._query is None:
      return self._cursor
    return self._query.cursor_after()


class KeyRangeKeyIterator(KeyRangeEntityIterator):
  """Yields ndb.Key type within a key range."""

  _KEYS_ONLY = True


class KeyRangeEntityProtoIterator(AbstractKeyRangeIterator):
  """Yields entities within a key range.

  Note: In the NDB world, this returns regular ndb entities rather than
  raw protocol buffer objects since NDB doesn't expose raw protos.
  """

  def __iter__(self):
    query = self._key_range.make_ascending_datastore_query(
        self._query_spec.entity_kind, filters=self._query_spec.filters)
    self._query = query.iter(
        start_cursor=self._cursor)
    yield from self._query

  def _get_cursor(self):
    if self._query is None:
      return self._cursor
    return self._query.cursor_after()


_KEY_RANGE_ITERATORS = {
    KeyRangeModelIterator.__name__: KeyRangeModelIterator,
    KeyRangeEntityIterator.__name__: KeyRangeEntityIterator,
    KeyRangeKeyIterator.__name__: KeyRangeKeyIterator,
    KeyRangeEntityProtoIterator.__name__: KeyRangeEntityProtoIterator
}
