#!/usr/bin/env python
# pylint: disable=g-bad-name

import datetime
import unittest

from google.cloud import ndb

from mapreduce import json_util


class FakeJsonType:

    def __init__(self, size=0):
        self.size = size

    def to_json(self):
        return {"size": self.size}

    @classmethod
    def from_json(cls, json):
        return cls(json["size"])


class EmptyDictJsonType:

    def to_json(self):
        return {}

    @classmethod
    def from_json(cls, _):
        return cls()


class FakeEntity(ndb.Model):
    json_property = json_util.JsonProperty(FakeJsonType)
    json_property_default_value = json_util.JsonProperty(
        FakeJsonType, default=FakeJsonType())
    empty_json_property = json_util.JsonProperty(EmptyDictJsonType)


class JsonSerializationTest(unittest.TestCase):

    def testE2e(self):
        now = datetime.datetime.now()
        obj = {"a": 1, "b": [{"c": "d"}], "e": now}
        new_obj = json_util.json.loads(json_util.json.dumps(
            obj, cls=json_util.JsonEncoder), cls=json_util.JsonDecoder)
        self.assertEqual(obj, new_obj)


class JsonPropertyTest(unittest.TestCase):

    def testToBaseType(self):
        e = FakeEntity()
        self.assertEqual(None, FakeEntity.json_property._to_base_type(e.json_property))
        e.json_property = FakeJsonType(5)
        self.assertEqual(
            '{"size": 5}', FakeEntity.json_property._to_base_type(e.json_property))

        e.empty_json_property = EmptyDictJsonType()
        self.assertEqual(
            None, FakeEntity.empty_json_property._to_base_type(e.empty_json_property))

    def testFromBaseType(self):
        self.assertEqual(
            None, FakeEntity.json_property._from_base_type(None))
        self.assertEqual(
            FakeJsonType,
            type(FakeEntity.json_property._from_base_type('{"size":4}')))
        self.assertTrue(
            4,
            FakeEntity.json_property._from_base_type('{"size":4}').size)

    def testValidate(self):
        self.assertRaises(
            ValueError,
            FakeEntity.json_property._validate, "a")

    def testDefaultValue(self):
        e = FakeEntity()
        self.assertEqual(None, e.json_property)
        self.assertTrue(e.json_property_default_value is not None)
