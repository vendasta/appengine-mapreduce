#!/usr/bin/env python
import os
import shutil
import tempfile
import time
import unittest
import json

from google.cloud import ndb

from mapreduce import errors
from mapreduce import handlers
from mapreduce import status
from testlib import testutil
from mapreduce import test_support


class FakeKind(ndb.Model):
  foobar = ndb.StringProperty(default="meep")


def FakeMap(entity):
  pass


class MapreduceYamlTest(unittest.TestCase):

  def set_up_directory_tree(self, dir_tree_contents):
    for full_path, contents in dir_tree_contents.items():
      dir_name = os.path.dirname(full_path)
      if not os.path.isdir(dir_name):
        os.makedirs(dir_name)
      f = open(full_path, 'w')
      f.write(contents)
      f.close()

  def setUp(self):
    self.tempdir = tempfile.mkdtemp()

  def tearDown(self):
    if self.tempdir:
      shutil.rmtree(self.tempdir)

  def testFindYamlFile(self):
    test_status = os.path.join(self.tempdir, "library_root", "google",
                               "appengine", "ext", "mapreduce", "status.py")
    test_mapreduce_yaml = os.path.join(self.tempdir, "application_root",
                                       "mapreduce.yaml")
    test_dict = {
        test_status: "test",
        test_mapreduce_yaml: "test",
    }
    self.set_up_directory_tree(test_dict)
    os.chdir(os.path.dirname(test_mapreduce_yaml))
    yaml_loc = status.find_mapreduce_yaml(status_file=test_status)
    self.assertTrue(os.path.samefile(test_mapreduce_yaml, yaml_loc))

  def testFindYamlFileSameTree(self):
    test_status = os.path.join(self.tempdir, "application_root", "google",
                               "appengine", "ext", "mapreduce", "status.py")
    test_mapreduce_yaml = os.path.join(self.tempdir, "application_root",
                                       "mapreduce.yaml")
    test_dict = {
        test_status: "test",
        test_mapreduce_yaml: "test",
    }
    self.set_up_directory_tree(test_dict)
    os.chdir(os.path.dirname(test_mapreduce_yaml))
    yaml_loc = status.find_mapreduce_yaml(status_file=test_status)
    self.assertEqual(test_mapreduce_yaml, yaml_loc)

  def testParseEmptyFile(self):
    self.assertRaises(errors.BadYamlError,
                      status.parse_mapreduce_yaml,
                      "")

  def testParse(self):
    mr_yaml = status.parse_mapreduce_yaml(
        "mapreduce:\n"
        "- name: Mapreduce1\n"
        "  mapper:\n"
        "    handler: Handler1\n"
        "    input_reader: Reader1\n"
        "    params_validator: Validator1\n"
        "    params:\n"
        "    - name: entity_kind\n"
        "      default: Kind1\n"
        "    - name: human_supplied1\n"
        "    - name: human_supplied2\n"
        "- name: Mapreduce2\n"
        "  mapper:\n"
        "    handler: Handler2\n"
        "    input_reader: Reader2\n")

    self.assertTrue(mr_yaml)
    self.assertEqual(2, len(mr_yaml.mapreduce))

    self.assertEqual("Mapreduce1", mr_yaml.mapreduce[0].name)
    self.assertEqual("Handler1", mr_yaml.mapreduce[0].mapper.handler)
    self.assertEqual("Reader1", mr_yaml.mapreduce[0].mapper.input_reader)
    self.assertEqual("Validator1",
                      mr_yaml.mapreduce[0].mapper.params_validator)
    self.assertEqual(3, len(mr_yaml.mapreduce[0].mapper.params))
    self.assertEqual("entity_kind", mr_yaml.mapreduce[0].mapper.params[0].name)
    self.assertEqual("Kind1", mr_yaml.mapreduce[0].mapper.params[0].default)
    self.assertEqual("human_supplied1",
                      mr_yaml.mapreduce[0].mapper.params[1].name)
    self.assertEqual("human_supplied2",
                      mr_yaml.mapreduce[0].mapper.params[2].name)

    self.assertEqual("Mapreduce2", mr_yaml.mapreduce[1].name)
    self.assertEqual("Handler2", mr_yaml.mapreduce[1].mapper.handler)
    self.assertEqual("Reader2", mr_yaml.mapreduce[1].mapper.input_reader)

  def testParseOutputWriter(self):
    mr_yaml = status.parse_mapreduce_yaml(
        "mapreduce:\n"
        "- name: Mapreduce1\n"
        "  mapper:\n"
        "    handler: Handler1\n"
        "    input_reader: Reader1\n"
        "    output_writer: Writer1\n"
        )

    self.assertTrue(mr_yaml)
    self.assertEqual(1, len(mr_yaml.mapreduce))

    self.assertEqual("Mapreduce1", mr_yaml.mapreduce[0].name)
    self.assertEqual("Handler1", mr_yaml.mapreduce[0].mapper.handler)
    self.assertEqual("Reader1", mr_yaml.mapreduce[0].mapper.input_reader)
    self.assertEqual("Writer1", mr_yaml.mapreduce[0].mapper.output_writer)

  def testParseMissingRequiredAttrs(self):
    self.assertRaises(errors.BadYamlError,
                      status.parse_mapreduce_yaml,
                      "mapreduce:\n"
                      "- name: Mapreduce1\n"
                      "  mapper:\n"
                      "    handler: Handler1\n")
    self.assertRaises(errors.BadYamlError,
                      status.parse_mapreduce_yaml,
                      "mapreduce:\n"
                      "- name: Mapreduce1\n"
                      "  mapper:\n"
                      "    input_reader: Reader1\n")

  def testBadValues(self):
    self.assertRaises(errors.BadYamlError,
                      status.parse_mapreduce_yaml,
                      "mapreduce:\n"
                      "- name: Mapreduce1\n"
                      "  mapper:\n"
                      "    handler: Handler1\n"
                      "    input_reader: Reader1\n"
                      "    params:\n"
                      "    - name: $$Invalid$$\n")

  def testMultipleDocuments(self):
    self.assertRaises(errors.MultipleDocumentsInMrYaml,
                      status.parse_mapreduce_yaml,
                      "mapreduce:\n"
                      "- name: Mapreduce1\n"
                      "  mapper:\n"
                      "    handler: Handler1\n"
                      "    input_reader: Reader1\n"
                      "---")

  def testOverlappingNames(self):
    self.assertRaises(errors.BadYamlError,
                      status.parse_mapreduce_yaml,
                      "mapreduce:\n"
                      "- name: Mapreduce1\n"
                      "  mapper:\n"
                      "    handler: Handler1\n"
                      "    input_reader: Reader1\n"
                      "- name: Mapreduce1\n"
                      "  mapper:\n"
                      "    handler: Handler1\n"
                      "    input_reader: Reader1\n")

  def testToDict(self):
    mr_yaml = status.parse_mapreduce_yaml(
        "mapreduce:\n"
        "- name: Mapreduce1\n"
        "  mapper:\n"
        "    handler: Handler1\n"
        "    input_reader: Reader1\n"
        "    params_validator: Validator1\n"
        "    params:\n"
        "    - name: entity_kind\n"
        "      default: Kind1\n"
        "    - name: human_supplied1\n"
        "    - name: human_supplied2\n"
        "- name: Mapreduce2\n"
        "  mapper:\n"
        "    handler: Handler2\n"
        "    input_reader: Reader2\n")
    all_configs = status.MapReduceYaml.to_dict(mr_yaml)
    self.assertEqual(
        [
          {
            'name': 'Mapreduce1',
            'mapper_params_validator': 'Validator1',
            'mapper_params': {
              'entity_kind': 'Kind1',
              'human_supplied2': None,
              'human_supplied1': None},
            'mapper_handler': 'Handler1',
            'mapper_input_reader': 'Reader1'
          },
          {
            'mapper_input_reader': 'Reader2',
            'mapper_handler': 'Handler2',
            'name': 'Mapreduce2'
          }
        ], all_configs)

  def testToDictOutputWriter(self):
    mr_yaml = status.parse_mapreduce_yaml(
        "mapreduce:\n"
        "- name: Mapreduce1\n"
        "  mapper:\n"
        "    handler: Handler1\n"
        "    input_reader: Reader1\n"
        "    output_writer: Writer1\n"
        )
    all_configs = status.MapReduceYaml.to_dict(mr_yaml)
    self.assertEqual(
        [
          {
            'name': 'Mapreduce1',
            'mapper_handler': 'Handler1',
            'mapper_input_reader': 'Reader1',
            'mapper_output_writer': 'Writer1',
          },
        ], all_configs)


class ResourceTest(testutil.HandlerTestBase):

  def testPaths(self):
    response = self.client.get("/mapreduce/status")
    self.assertEqual(200, response.status_code)
    self.assertTrue(response.data.startswith(b"<!DOCTYPE html>"))
    self.assertEqual("text/html", response.headers["Content-Type"])

    response = self.client.get("/mapreduce/jquery.js")
    self.assertEqual(200, response.status_code)
    self.assertTrue(response.data.startswith(b"/*!"))
    self.assertEqual("text/javascript", response.headers["Content-Type"])

  def testCachingHeaders(self):
    response = self.client.get("/mapreduce/status")
    self.assertEqual("public; max-age=300",
                      response.headers["Cache-Control"])

  def testMissing(self):
    response = self.client.get("/mapreduce/missing")
    self.assertEqual(404, response.status_code)


class ListConfigsTest(testutil.HandlerTestBase):

  def testCSRF(self):
    response = self.client.get("/mapreduce/command/list_configs")
    self.assertEqual(403, response.status_code)

  def testBasic(self):
    old_get_yaml = status.get_mapreduce_yaml
    status.get_mapreduce_yaml = lambda: status.parse_mapreduce_yaml(
        "mapreduce:\n"
        "- name: Mapreduce1\n"
        "  mapper:\n"
        "    handler: Handler1\n"
        "    input_reader: Reader1\n"
        "    params_validator: Validator1\n"
        "    params:\n"
        "    - name: entity_kind\n"
        "      default: Kind1\n"
        "    - name: human_supplied1\n"
        "    - name: human_supplied2\n"
        "- name: Mapreduce2\n"
        "  mapper:\n"
        "    handler: Handler2\n"
        "    input_reader: Reader2\n"
        "  params_validator: MapreduceValidator\n"
        "  params:\n"
        "  - name: foo\n"
        "    value: bar\n")
    try:
      response = self.client.get("/mapreduce/command/list_configs", headers={
          "X-Requested-With": "XMLHttpRequest",
      })
    finally:
      status.get_mapreduce_yaml = old_get_yaml

    self.assertEqual("text/javascript", response.headers["Content-Type"])

    self.assertEqual(
        {'configs': [
          {'mapper_params_validator': 'Validator1',
           'mapper_params': {
              'entity_kind': 'Kind1',
              'human_supplied2': None,
              'human_supplied1': None},
            'mapper_input_reader': 'Reader1',
            'mapper_handler': 'Handler1',
            'name': 'Mapreduce1'},
          {'mapper_input_reader': 'Reader2',
           'mapper_handler': 'Handler2',
           'name': 'Mapreduce2',
           'params': {
               'foo': 'bar',},
           }]},
        json.loads(response.data))


class ListJobsTest(testutil.HandlerTestBase):

    def testCSRF(self):
        FakeKind().put()

        response = self.client.post("/mapreduce/command/start_job")
        self.assertEqual(403, response.status_code)

        response = self.client.get("/mapreduce/command/list_jobs")
        self.assertEqual(403, response.status_code)

    def testBasic(self):
        FakeKind().put()

        for job_name in ["my job 1", "my job 2", "my job 3"]:
          self.client.post(
            "/mapreduce/command/start_job",
            data={
              "name": job_name,
              "mapper_input_reader": "mapreduce.input_readers.DatastoreInputReader",
              "mapper_handler": f"{FakeMap.__module__}.{FakeMap.__name__}",
              "mapper_params.entity_kind": f"{FakeKind.__module__}.{FakeKind.__name__}",
            },
            headers={
              "X-Requested-With": "XMLHttpRequest",
            },
          )
          time.sleep(0.1)

        response = self.client.get(
            "/mapreduce/command/list_jobs",
            headers={
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        result = json.loads(response.data)
        expected_args = {
                "active",
                "active_shards",
                "chart_url",
                "chart_width",
                "mapreduce_id",
                "name",
                "shards",
                "start_timestamp_ms",
                "updated_timestamp_ms",
        }
        self.assertEqual(3, len(result["jobs"]))
        self.assertEqual("my job 3", result["jobs"][0]["name"])
        self.assertEqual("my job 2", result["jobs"][1]["name"])
        self.assertEqual("my job 1", result["jobs"][2]["name"])
        self.assertEqual(expected_args, set(result["jobs"][0].keys()))
        self.assertEqual(expected_args, set(result["jobs"][1].keys()))
        self.assertEqual(expected_args, set(result["jobs"][2].keys()))

    def testCursor(self):
        FakeKind().put()
        for job_name in ["my job 1", "my job 2"]:
          self.client.post(
            "/mapreduce/command/start_job",
            data={
              "name": job_name,
              "mapper_input_reader": "mapreduce.input_readers.DatastoreInputReader",
              "mapper_handler": f"{FakeMap.__module__}.{FakeMap.__name__}",
              "mapper_params.entity_kind": f"{FakeKind.__module__}.{FakeKind.__name__}",
            },
            headers={
              "X-Requested-With": "XMLHttpRequest",
            },
          )
          time.sleep(0.1)

        response = self.client.get(
            "/mapreduce/command/list_jobs?count=1",
            headers={
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        result = json.loads(response.data)
        self.assertEqual(1, len(result["jobs"]))
        self.assertTrue("cursor" in result)

        response2 = self.client.get(
            "/mapreduce/command/list_jobs?count=1&cursor=" + result["cursor"],
            headers={
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        result2 = json.loads(response2.data)
        self.assertEqual(1, len(result2["jobs"]))
        self.assertFalse("cursor" in result2)

    def testNoJobs(self):
        response = self.client.get(
            "/mapreduce/command/list_jobs",
            headers={
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        result = json.loads(response.data)
        self.assertEqual({'jobs': []}, result)


class GetJobDetailTest(testutil.HandlerTestBase):

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)

    for _ in range(100):
      FakeKind().put()

    response = self.client.post(
      "/mapreduce/command/start_job",
      data={
        "name": "my job 1",
        "mapper_input_reader": "mapreduce.input_readers.DatastoreInputReader",
        "mapper_handler": f"{FakeMap.__module__}.{FakeMap.__name__}",
        "mapper_params.entity_kind": f"{FakeKind.__module__}.{FakeKind.__name__}",
      },
      headers={
        "X-Requested-With": "XMLHttpRequest",
      },
    )
    self.mapreduce_id = json.loads(response.data)["mapreduce_id"]

  def KickOffMapreduce(self):
    test_support.execute_all_tasks(self.taskqueue)

  def testCSRF(self):
    response = self.client.get("/mapreduce/command/get_job_detail")
    self.assertEqual(403, response.status_code)

  def testBasic(self):
    self.KickOffMapreduce()

    response = self.client.get(
        "/mapreduce/command/get_job_detail?mapreduce_id=" + self.mapreduce_id,
        headers={
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    result = json.loads(response.data)

    expected_keys = {
        "active", "chart_url", "counters", "mapper_spec", "mapreduce_id",
        "name", "result_status", "shards", "start_timestamp_ms",
        "updated_timestamp_ms", "params", "hooks_class_name", "chart_width"}
    expected_shard_keys = {
        "active", "counters", "last_work_item", "result_status",
        "shard_description", "shard_id", "shard_number",
        "updated_timestamp_ms"}

    self.assertEqual(expected_keys, set(result.keys()))
    self.assertEqual(8, len(result["shards"]))
    self.assertEqual(expected_shard_keys, set(result["shards"][0].keys()))

  def testBeforeKickOff(self):
    response = self.client.get(
        "/mapreduce/command/get_job_detail?mapreduce_id=" + self.mapreduce_id,
        headers={
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    result = json.loads(response.data)

    expected_keys = {
        "active", "chart_url", "counters", "mapper_spec", "mapreduce_id",
        "name", "result_status", "shards", "start_timestamp_ms",
        "updated_timestamp_ms", "params", "hooks_class_name", "chart_width"}

    self.assertEqual(expected_keys, set(result.keys()))

  def testBadJobId(self):
    response = self.client.get(
        "/mapreduce/command/get_job_detail?mapreduce_id=does_not_exist",
        headers={
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    result = json.loads(response.data)
    self.assertEqual(
        {"error_message": "\"Could not find job with ID 'does_not_exist'\"",
         "error_class": "KeyError"},
        result)
