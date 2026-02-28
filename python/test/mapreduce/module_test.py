#!/usr/bin/env python
#
# Copyright 2010 Google Inc. All Rights Reserved.

import unittest

from mapreduce import context
from mapreduce import control
from mapreduce import datastore_range_iterators
from mapreduce import errors
from mapreduce import input_readers
from mapreduce import key_ranges
from mapreduce import mapper_pipeline
from mapreduce import mapreduce_pipeline
from mapreduce import model
from mapreduce import namespace_range
from mapreduce import operation as op
from mapreduce import output_writers
from mapreduce import property_range
from mapreduce import records
from mapreduce import shuffler
from mapreduce import util


class NamespaceRangeTest(unittest.TestCase):
  MODULE = namespace_range

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class PropertyRangeTest(unittest.TestCase):
  MODULE = property_range

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class KeyRangesTest(unittest.TestCase):
  MODULE = key_ranges

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class DatastoreRangeIteratorsTest(unittest.TestCase):
  MODULE = datastore_range_iterators

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class ContextTest(unittest.TestCase):
  MODULE = context

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class ControlTest(unittest.TestCase):
  MODULE = control

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class CountersTest(unittest.TestCase):
  MODULE = op.counters

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class DbTest(unittest.TestCase):
  MODULE = op.db

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class ErrorsTest(unittest.TestCase):
  MODULE = errors

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class InputReadersTest(unittest.TestCase):
  MODULE = input_readers

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class ModelTest(unittest.TestCase):
  MODULE = model

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class OutputWritersTest(unittest.TestCase):
  MODULE = output_writers

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class UtilTest(unittest.TestCase):
  MODULE = util

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class MapperPipelineTest(unittest.TestCase):
  MODULE = mapper_pipeline

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class MapreducePipelineTest(unittest.TestCase):
  MODULE = mapreduce_pipeline

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class ShufflerTest(unittest.TestCase):
  MODULE = shuffler

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)


class RecordsTests(unittest.TestCase):
  MODULE = records

  def test_module_importable(self):
    self.assertIsNotNone(self.MODULE)
