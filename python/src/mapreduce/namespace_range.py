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

"""Represents a lexographic range of namespaces."""


# pylint: disable=g-bad-name

__all__ = [
    'NAMESPACE_CHARACTERS',
    'MAX_NAMESPACE_LENGTH',
    'MAX_NAMESPACE',
    'MIN_NAMESPACE',
    'NAMESPACE_BATCH_SIZE',
    'NamespaceRange',
    'get_namespace_keys',
]

import itertools
import string

from google.cloud import ndb
from google.cloud import datastore as cloud_datastore

_NAMESPACE_KIND = '__namespace__'
_EMPTY_NAMESPACE_ID = 1

NAMESPACE_CHARACTERS = "".join(
    sorted(string.digits + string.ascii_lowercase + string.ascii_uppercase + "._-")
)
MAX_NAMESPACE_LENGTH = 100
MIN_NAMESPACE = ''
NAMESPACE_BATCH_SIZE = 50


def _setup_constants(alphabet=NAMESPACE_CHARACTERS,
                     max_length=MAX_NAMESPACE_LENGTH,
                     batch_size=NAMESPACE_BATCH_SIZE):
  """Calculate derived constant values. Only useful for testing."""

  global NAMESPACE_CHARACTERS
  global MAX_NAMESPACE_LENGTH
  # pylint: disable=global-variable-undefined
  global MAX_NAMESPACE
  global _LEX_DISTANCE
  global NAMESPACE_BATCH_SIZE

  NAMESPACE_CHARACTERS = alphabet
  MAX_NAMESPACE_LENGTH = max_length
  MAX_NAMESPACE = NAMESPACE_CHARACTERS[-1] * MAX_NAMESPACE_LENGTH
  NAMESPACE_BATCH_SIZE = batch_size

  _LEX_DISTANCE = [1]
  for i in range(1, MAX_NAMESPACE_LENGTH):
    _LEX_DISTANCE.append(
        _LEX_DISTANCE[i-1] * len(NAMESPACE_CHARACTERS) + 1)
  # pylint: disable=undefined-loop-variable
  del i
_setup_constants()


def _ord_to_namespace(n, _max_length=None):
  """Convert a namespace ordinal to a namespace string."""
  if _max_length is None:
    _max_length = MAX_NAMESPACE_LENGTH

  length = _LEX_DISTANCE[_max_length - 1]
  if n == 0:
    return ''
  n -= 1
  return (NAMESPACE_CHARACTERS[n // length] +
          _ord_to_namespace(n % length, _max_length - 1))


def _namespace_to_ord(namespace):
  """Converts a namespace string into an int representing its lexographic order."""
  n = 0
  for i, c in enumerate(namespace):
    n += (_LEX_DISTANCE[MAX_NAMESPACE_LENGTH - i- 1] *
          NAMESPACE_CHARACTERS.index(c)
          + 1)
  return n


def _key_for_namespace(namespace, app):
  """Return the __namespace__ key for a namespace."""
  client = cloud_datastore.Client(project=app)
  if namespace:
    return client.key(_NAMESPACE_KIND, namespace)
  else:
    return client.key(_NAMESPACE_KIND, _EMPTY_NAMESPACE_ID)


class NamespaceRange:
  """An inclusive lexographical range of namespaces.

  This class is immutable.
  """

  def __init__(self,
               namespace_start=None,
               namespace_end=None,
               _app=None):
    if namespace_start is None:
      namespace_start = MIN_NAMESPACE

    if namespace_end is None:
      namespace_end = MAX_NAMESPACE

    if namespace_start > namespace_end:
      raise ValueError('namespace_start ({!r}) > namespace_end ({!r})'.format(
          namespace_start, namespace_end))
    self.__namespace_start = namespace_start
    self.__namespace_end = namespace_end
    self.__app = _app

  @property
  def app(self):
    return self.__app

  @property
  def namespace_start(self):
    return self.__namespace_start

  @property
  def namespace_end(self):
    return self.__namespace_end

  @property
  def is_single_namespace(self):
    """True if the namespace range only includes a single namespace."""
    return self.namespace_start == self.namespace_end

  def split_range(self):
    """Splits the NamespaceRange into two nearly equal-sized ranges."""
    if self.is_single_namespace:
      return [self]

    mid_point = (_namespace_to_ord(self.namespace_start) +
                 _namespace_to_ord(self.namespace_end)) // 2

    return [NamespaceRange(self.namespace_start,
                           _ord_to_namespace(mid_point),
                           _app=self.app),
            NamespaceRange(_ord_to_namespace(mid_point+1),
                           self.namespace_end,
                           _app=self.app)]

  def __copy__(self):
    return self.__class__(self.__namespace_start,
                          self.__namespace_end,
                          self.__app)

  def __eq__(self, o):
    return (self.namespace_start == o.namespace_start and
            self.namespace_end == o.namespace_end)

  def __hash__(self):
    return hash((self.namespace_start, self.namespace_end, self.app))

  def __repr__(self):
    if self.app is None:
      return 'NamespaceRange(namespace_start={!r}, namespace_end={!r})'.format(
          self.namespace_start, self.namespace_end)
    else:
      return 'NamespaceRange(namespace_start={!r}, namespace_end={!r}, _app={!r})'.format(
          self.namespace_start, self.namespace_end, self.app)

  def with_start_after(self, after_namespace):
    """Returns a copy of this NamespaceName with a new namespace_start."""
    namespace_start = _ord_to_namespace(_namespace_to_ord(after_namespace) + 1)
    return NamespaceRange(namespace_start, self.namespace_end, _app=self.app)

  def _query_namespaces(self, limit=None):
    """Query __namespace__ entities using Cloud Datastore client.

    Returns:
      A list of namespace key objects.
    """
    client = cloud_datastore.Client()
    query = client.query(kind=_NAMESPACE_KIND)

    start_key = _key_for_namespace(self.namespace_start, self.app)
    end_key = _key_for_namespace(self.namespace_end, self.app)

    query.key_filter(start_key, '>=')
    query.key_filter(end_key, '<=')
    query.keys_only()

    results = list(query.fetch(limit=limit))
    return results

  def normalized_start(self):
    """Returns a NamespaceRange with leading non-existant namespaces removed."""
    results = self._query_namespaces(limit=1)

    if not results:
      return None

    ns_name = results[0].key.name or ''
    return NamespaceRange(ns_name,
                          self.namespace_end,
                          _app=self.app)

  def to_json_object(self):
    """Returns a dict representation that can be serialized to JSON."""
    obj_dict = dict(namespace_start=self.namespace_start,
                    namespace_end=self.namespace_end)
    if self.app is not None:
      obj_dict['app'] = self.app
    return obj_dict

  @classmethod
  def from_json_object(cls, json):
    """Returns a NamespaceRange from an object deserialized from JSON."""
    return cls(json['namespace_start'],
               json['namespace_end'],
               _app=json.get('app'))

  @classmethod
  def split(cls,
            n,
            contiguous,
            can_query=itertools.chain(itertools.repeat(True, 50),
                                      itertools.repeat(False)).__next__,
            _app=None):
    if n < 1:
      raise ValueError('n must be >= 1')

    ranges = None
    if can_query():
      if not contiguous:
        ns_keys = get_namespace_keys(_app, n + 1)
        if not ns_keys:
          return []
        else:
          if len(ns_keys) <= n:
            ns_range = []
            for ns_key in ns_keys:
              ns_range.append(NamespaceRange(ns_key.name or '',
                                             ns_key.name or '',
                                             _app=_app))
            return sorted(ns_range,
                          key=lambda ns_range: ns_range.namespace_start)
          ranges = [NamespaceRange(ns_keys[0].name or '', _app=_app)]
      else:
        ns_range = NamespaceRange(_app=_app).normalized_start()
        if ns_range is None:
          return [NamespaceRange(_app=_app)]
        ranges = [ns_range]
    else:
      ranges = [NamespaceRange(_app=_app)]

    singles = []
    while ranges and (len(ranges) + len(singles)) < n:
      namespace_range = ranges.pop(0)
      if namespace_range.is_single_namespace:
        singles.append(namespace_range)
      else:
        left, right = namespace_range.split_range()
        if can_query():
          right = right.normalized_start()
        if right is not None:
          ranges.append(right)
        ranges.append(left)
    ns_ranges = sorted(singles + ranges,
                       key=lambda ns_range: ns_range.namespace_start)

    if contiguous:
      if not ns_ranges:
        return [NamespaceRange(_app=_app)]

      continuous_ns_ranges = []
      for i in range(len(ns_ranges)):
        if i == 0:
          namespace_start = MIN_NAMESPACE
        else:
          namespace_start = ns_ranges[i].namespace_start

        if i == len(ns_ranges) - 1:
          namespace_end = MAX_NAMESPACE
        else:
          namespace_end = _ord_to_namespace(
              _namespace_to_ord(ns_ranges[i+1].namespace_start) - 1)

        continuous_ns_ranges.append(NamespaceRange(namespace_start,
                                                   namespace_end,
                                                   _app=_app))
      return continuous_ns_ranges
    else:
      return ns_ranges

  def __iter__(self):
    """Iterate over all the namespaces within this range."""
    results = self._query_namespaces(limit=None)
    for entity in results:
      yield entity.key.name or ''


def get_namespace_keys(app, limit):
  """Get namespace keys."""
  client = cloud_datastore.Client()
  query = client.query(kind=_NAMESPACE_KIND)
  query.keys_only()
  return [entity.key for entity in query.fetch(limit=limit)]
