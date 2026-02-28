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

"""Tests for mapreduce.namespace_range."""

import os
import unittest
from unittest import mock

from google.cloud import ndb
from google.cloud import datastore as cloud_datastore

from mapreduce import namespace_range


class Pet(ndb.Model):
  pass


class TestOrdinalization(unittest.TestCase):

  def setUp(self):
    unittest.TestCase.setUp(self)

  def tearDown(self):
    namespace_range._setup_constants()
    unittest.TestCase.tearDown(self)

  def testWithSmallRange(self):
    namespace_range._setup_constants('ab', 2)
    self.assertEqual('', namespace_range._ord_to_namespace(0))
    self.assertEqual(0, namespace_range._namespace_to_ord(''))

    self.assertEqual('a', namespace_range._ord_to_namespace(1))
    self.assertEqual(1, namespace_range._namespace_to_ord('a'))

    self.assertEqual('aa', namespace_range._ord_to_namespace(2))
    self.assertEqual(2, namespace_range._namespace_to_ord('aa'))

    self.assertEqual('ab', namespace_range._ord_to_namespace(3))
    self.assertEqual(3, namespace_range._namespace_to_ord('ab'))

    self.assertEqual('b', namespace_range._ord_to_namespace(4))
    self.assertEqual(4, namespace_range._namespace_to_ord('b'))

    self.assertEqual('ba', namespace_range._ord_to_namespace(5))
    self.assertEqual(5, namespace_range._namespace_to_ord('ba'))

    self.assertEqual('bb', namespace_range._ord_to_namespace(6))
    self.assertEqual(6, namespace_range._namespace_to_ord('bb'))


class JsonTest(unittest.TestCase):

  def testToJsonObject(self):
    self.assertEqual(
        dict(namespace_start='a',
             namespace_end='b'),
        namespace_range.NamespaceRange('a', 'b').to_json_object())

  def testToJsonObjectWithApp(self):
    self.assertEqual(
        dict(namespace_start='a',
             namespace_end='b',
             app='myapp'),
        namespace_range.NamespaceRange('a', 'b', _app='myapp').to_json_object())

  def testFromJsonObject(self):
    self.assertEqual(
        namespace_range.NamespaceRange('a', 'b'),
        namespace_range.NamespaceRange.from_json_object(
            dict(namespace_start='a',
                 namespace_end='b')))

  def testFromJsonObjectWithApp(self):
    self.assertEqual(
        namespace_range.NamespaceRange('a', 'b', _app='myapp'),
        namespace_range.NamespaceRange.from_json_object(
            dict(namespace_start='a',
                 namespace_end='b',
                 app='myapp')))


class NamespaceRangeIterationTest(unittest.TestCase):

  def setUp(self):
    unittest.TestCase.setUp(self)
    namespace_range._setup_constants('abc', 3, 3)
    self.app_id = 'testbed-test'
    os.environ['APPLICATION_ID'] = self.app_id

  def tearDown(self):
    namespace_range._setup_constants()
    unittest.TestCase.tearDown(self)

  def testQueryPaging(self):
    with mock.patch.object(cloud_datastore.Client, 'query') as mock_query_method:
      ns_range = namespace_range.NamespaceRange(
          namespace_start='a', namespace_end='b', _app=self.app_id)

      def make_key(ns):
        client = cloud_datastore.Client()
        return client.key('__namespace__', ns)

      mock_query_obj = mock.Mock()
      mock_query_method.return_value = mock_query_obj

      ns_keys = ['a', 'aa', 'aaa', 'aab', 'ab', 'ac', 'b']

      def make_entity(ns):
        client = cloud_datastore.Client()
        e = cloud_datastore.Entity(key=client.key('__namespace__', ns))
        return e

      mock_query_obj.fetch.return_value = [make_entity(ns) for ns in ns_keys]
      mock_query_obj.keys_only = mock.Mock()
      mock_query_obj.key_filter = mock.Mock()

      self.assertEqual(7, len(list(ns_range)))


class NamespaceRangeSplitTest(unittest.TestCase):

  def setUp(self):
    unittest.TestCase.setUp(self)
    namespace_range._setup_constants('abc', 3, 3)
    self.app_id = 'testbed-test'
    os.environ['APPLICATION_ID'] = self.app_id

  def tearDown(self):
    namespace_range._setup_constants()
    unittest.TestCase.tearDown(self)

  def CreateInNamespace(self, namespace):
    pet = Pet(namespace=namespace)
    pet.put()

  def testSplitWithoutQueries(self):
    self.assertEqual(
        [namespace_range.NamespaceRange(namespace_start='',
                                        namespace_end='abc',
                                        _app=self.app_id),
         namespace_range.NamespaceRange(namespace_start='ac',
                                        namespace_end='bb',
                                        _app=self.app_id),
         namespace_range.NamespaceRange(namespace_start='bba',
                                        namespace_end='caa',
                                        _app=self.app_id),
         namespace_range.NamespaceRange(namespace_start='cab',
                                        namespace_end='ccc',
                                        _app=self.app_id)],
        namespace_range.NamespaceRange.split(4,
                                             contiguous=False,
                                             can_query=lambda: False,
                                             _app=self.app_id))

  def testSplitWithoutQueriesWithContiguous(self):
    self.assertEqual(
        [namespace_range.NamespaceRange(namespace_start='',
                                        namespace_end='abc',
                                        _app=self.app_id),
         namespace_range.NamespaceRange(namespace_start='ac',
                                        namespace_end='bb',
                                        _app=self.app_id),
         namespace_range.NamespaceRange(namespace_start='bba',
                                        namespace_end='caa',
                                        _app=self.app_id),
         namespace_range.NamespaceRange(namespace_start='cab',
                                        namespace_end='ccc',
                                        _app=self.app_id)],
        namespace_range.NamespaceRange.split(4,
                                             contiguous=True,
                                             can_query=lambda: False,
                                             _app=self.app_id))
