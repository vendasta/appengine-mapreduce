"""Replacement for google.appengine.ext.key_range using NDB."""

from google.cloud import ndb


class KeyRange:
  """Represents a range of keys for a given entity kind."""

  ASC = "ASC"
  DESC = "DESC"

  def __init__(self,
               key_start=None,
               key_end=None,
               direction=None,
               include_start=True,
               include_end=True,
               namespace=None,
               _app=None):
    self.key_start = key_start
    self.key_end = key_end
    self.direction = direction or self.ASC
    self.include_start = include_start
    self.include_end = include_end
    self.namespace = namespace
    self._app = _app

  def advance(self, key):
    """Updates the start of this range to be past the given key."""
    self.include_start = False
    self.key_start = key

  def make_ascending_query(self, model_class, filters=None):
    """Creates an NDB query for the given model class within this key range.

    Args:
      model_class: an ndb.Model subclass.
      filters: optional list of (property_name, operator, value) tuples.

    Returns:
      An ndb.Query.
    """
    query = model_class.query(namespace=self.namespace)

    if self.key_start is not None:
      if self.include_start:
        query = query.filter(model_class.key >= self.key_start)
      else:
        query = query.filter(model_class.key > self.key_start)

    if self.key_end is not None:
      if self.include_end:
        query = query.filter(model_class.key <= self.key_end)
      else:
        query = query.filter(model_class.key < self.key_end)

    if filters:
      for prop_name, op, value in filters:
        query = query.filter(ndb.FilterNode(prop_name, op, value))

    query = query.order(model_class.key)
    return query

  def make_ascending_datastore_query(self, kind, filters=None):
    """Creates an NDB query using kind string within this key range.

    For compatibility — returns the same type as make_ascending_query but
    constructs a kind-based query without a specific model class.

    Args:
      kind: entity kind string.
      filters: optional list of (property_name, operator, value) tuples.

    Returns:
      An ndb.Query.
    """
    from mapreduce import util
    model_class = util.for_name(kind)
    if model_class and isinstance(model_class, type) and issubclass(model_class, ndb.Model):
      return self.make_ascending_query(model_class, filters=filters)

    query = ndb.Query(kind=kind, namespace=self.namespace)

    if self.key_start is not None:
      if self.include_start:
        query = query.filter(ndb.FilterNode('__key__', '>=', self.key_start))
      else:
        query = query.filter(ndb.FilterNode('__key__', '>', self.key_start))

    if self.key_end is not None:
      if self.include_end:
        query = query.filter(ndb.FilterNode('__key__', '<=', self.key_end))
      else:
        query = query.filter(ndb.FilterNode('__key__', '<', self.key_end))

    if filters:
      for prop_name, op, value in filters:
        query = query.filter(ndb.FilterNode(prop_name, op, value))

    query = query.order(ndb.Key)
    return query

  def to_json(self):
    result = {
        "direction": self.direction,
        "include_start": self.include_start,
        "include_end": self.include_end,
        "namespace": self.namespace,
    }
    if self.key_start is not None:
      result["key_start"] = self.key_start.urlsafe().decode()
    else:
      result["key_start"] = None
    if self.key_end is not None:
      result["key_end"] = self.key_end.urlsafe().decode()
    else:
      result["key_end"] = None
    if self._app is not None:
      result["_app"] = self._app
    return result

  @classmethod
  def from_json(cls, json_obj):
    key_start = None
    key_end = None
    if json_obj.get("key_start"):
      key_start = ndb.Key(urlsafe=json_obj["key_start"])
    if json_obj.get("key_end"):
      key_end = ndb.Key(urlsafe=json_obj["key_end"])

    return cls(
        key_start=key_start,
        key_end=key_end,
        direction=json_obj.get("direction", cls.ASC),
        include_start=json_obj.get("include_start", True),
        include_end=json_obj.get("include_end", True),
        namespace=json_obj.get("namespace"),
        _app=json_obj.get("_app"))

  def __repr__(self):
    return "KeyRange(key_start=%r, key_end=%r, namespace=%r)" % (
        self.key_start, self.key_end, self.namespace)
