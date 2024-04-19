#!/usr/bin/env python
# Disable "Invalid method name"
# pylint: disable=g-bad-name

import datetime
import os
import sys
import unittest

from google.appengine.api import datastore_errors
from google.appengine.ext import db

# Fix up paths for running tests.
sys.path.append(os.path.join(os.path.dirname(__file__), "../../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from mapreduce import json_util


class TestJsonType(object):
  """Test class with to_json/from_json methods."""

  def __init__(self, size=0):
    self.size = size

  def to_json(self):
    return {"size": self.size}

  @classmethod
  def from_json(cls, json):
    return cls(json["size"])


class EmptyDictJsonType(object):
  """Test class which serializes to empty json dict."""

  def to_json(self):
    return {}

  @classmethod
  def from_json(cls, _):
    return cls()


class TestEntity(db.Model):
  """Test entity class."""

  json_property = json_util.JsonProperty(TestJsonType)
  json_property_default_value = json_util.JsonProperty(
      TestJsonType, default=TestJsonType())
  empty_json_property = json_util.JsonProperty(EmptyDictJsonType)


class JsonSerializationTest(unittest.TestCase):
  """Test custom json encoder and decoder."""

  def testE2e(self):
    now = datetime.datetime.now()
    obj = {"a": 1, "b": [{"c": "d"}], "e": now}
    new_obj = json_util.json.loads(json_util.json.dumps(
        obj, cls=json_util.JsonEncoder), cls=json_util.JsonDecoder)
    self.assertEqual(obj, new_obj)


class JsonPropertyTest(unittest.TestCase):
  """Test model.JsonProperty."""

  def testGetValueForDatastore(self):
    """Test get_value_for_datastore method."""
    e = TestEntity()
    self.assertEqual(None, TestEntity.json_property.get_value_for_datastore(e))
    e.json_property = TestJsonType(5)
    self.assertEqual(
        '{"size": 5}', TestEntity.json_property.get_value_for_datastore(e))

    e.empty_json_property = EmptyDictJsonType()
    self.assertEqual(
        None, TestEntity.empty_json_property.get_value_for_datastore(e))

  def testMakeValueFromDatastore(self):
    """Test make_value_from_datastore method."""
    self.assertEqual(
        None, TestEntity.json_property.make_value_from_datastore(None))
    self.assertEqual(
        TestJsonType,
        type(TestEntity.json_property.make_value_from_datastore('{"size":4}')))
    self.assertTrue(
        4,
        TestEntity.json_property.make_value_from_datastore('{"size":4}').size)

  def testValidate(self):
    """Test validate method."""
    self.assertRaises(
        datastore_errors.BadValueError,
        TestEntity.json_property.validate, "a")

  def testEmpty(self):
    """Test empty() method."""
    self.assertTrue(TestEntity.json_property.empty(None))
    self.assertFalse(TestEntity.json_property.empty("abcd"))

  def testDefaultValue(self):
    """Test default value."""
    e = TestEntity()
    self.assertEqual(None, e.json_property)
    self.assertTrue(e.json_property_default_value is not None)


if __name__ == "__main__":
  unittest.main()
