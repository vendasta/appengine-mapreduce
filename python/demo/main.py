#!/usr/bin/env python
#
# Copyright 2011 Google Inc.
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

""" This is a sample application that tests the MapReduce API.

It does so by allowing users to upload a zip file containing plaintext files
and perform some kind of analysis upon it. Currently three types of MapReduce
jobs can be run over user-supplied input data: a WordCount MR that reports the
number of occurrences of each word, an Index MR that reports which file(s) each
word in the input corpus comes from, and a Phrase MR that finds statistically
improbably phrases for a given input file (this requires many input files in the
zip file to attain higher accuracies)."""

__author__ = """aizatsky@google.com (Mike Aizatsky), cbunch@google.com (Chris
Bunch)"""

# Using opensource naming conventions, pylint: disable=g-bad-name

import datetime
import logging
import os
import re
import sys

from flask import Flask, redirect, render_template, request, url_for
from flask.views import MethodView
from google.appengine.api import app_identity, users, wrap_wsgi_app
from google.appengine.ext import blobstore, db

sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

import mapreduce
from mapreduce import base_handler, mapreduce_pipeline

app = Flask(__name__)
app.wsgi_app = wrap_wsgi_app(app.wsgi_app, use_legacy_context_mode=True)

project_name = os.getenv('MAPREDUCE_TEST_PROJECT', app_identity.get_application_id())
bucket_name = os.getenv('MAPREDUCE_TEST_BUCKET', app_identity.get_default_gcs_bucket_name())

class FileMetadata(db.Model):
  """A helper class that will hold metadata for the user's blobs.

  Specifially, we want to keep track of who uploaded it, where they uploaded it
  from (right now they can only upload from their computer, but in the future
  urlfetch would be nice to add), and links to the results of their MR jobs. To
  enable our querying to scan over our input data, we store keys in the form
  'user/date/blob_key', where 'user' is the given user's e-mail address, 'date'
  is the date and time that they uploaded the item on, and 'blob_key'
  indicates the location in the Blobstore that the item can be found at. '/'
  is not the actual separator between these values - we use '..' since it is
  an illegal set of characters for an e-mail address to contain.
  """

  __SEP = ".."
  __NEXT = "./"

  owner = db.UserProperty()
  filename = db.StringProperty()
  uploadedOn = db.DateTimeProperty()
  source = db.StringProperty()
  blobkey = db.StringProperty()
  wordcount_link = db.StringProperty()
  index_link = db.StringProperty()
  phrases_link = db.StringProperty()

  @staticmethod
  def getFirstKeyForUser(username):
    """Helper function that returns the first possible key a user could own.

    This is useful for table scanning, in conjunction with getLastKeyForUser.

    Args:
      username: The given user's e-mail address.
    Returns:
      The internal key representing the earliest possible key that a user could
      own (although the value of this key is not able to be used for actual
      user data).
    """

    return db.Key.from_path("FileMetadata", username + FileMetadata.__SEP)

  @staticmethod
  def getLastKeyForUser(username):
    """Helper function that returns the last possible key a user could own.

    This is useful for table scanning, in conjunction with getFirstKeyForUser.

    Args:
      username: The given user's e-mail address.
    Returns:
      The internal key representing the last possible key that a user could
      own (although the value of this key is not able to be used for actual
      user data).
    """

    return db.Key.from_path("FileMetadata", username + FileMetadata.__NEXT)

  @staticmethod
  def getKeyName(username, date, blob_key):
    """Returns the internal key for a particular item in the database.

    Our items are stored with keys of the form 'user/date/blob_key' ('/' is
    not the real separator, but __SEP is).

    Args:
      username: The given user's e-mail address.
      date: A datetime object representing the date and time that an input
        file was uploaded to this app.
      blob_key: The blob key corresponding to the location of the input file
        in the Blobstore.
    Returns:
      The internal key for the item specified by (username, date, blob_key).
    """

    sep = FileMetadata.__SEP
    return str(username + sep + str(date) + sep + blob_key)


class IndexHandler(MethodView):
    def get(self):
        user = users.get_current_user()
        username = user.nickname()

        first = FileMetadata.getFirstKeyForUser(username)
        last = FileMetadata.getLastKeyForUser(username)

        q = FileMetadata.all()
        q.filter("__key__ >", first)
        q.filter("__key__ < ", last)
        results = q.fetch(10)

        items = [result for result in results]
        length = len(items)

        upload_url = blobstore.create_upload_url("/upload", gs_bucket_name=bucket_name)

        return render_template("index.html", username=username, items=items, length=length, upload_url=upload_url)

    def post(self):
        filekey = request.form.get("filekey")
        blob_key = request.form.get("blobkey")

        if request.form.get("word_count"):
            pipeline = WordCountPipeline(filekey, blob_key)
        elif request.form.get("index"):
            pipeline = IndexPipeline(filekey, blob_key)
        else:
            pipeline = PhrasesPipeline(filekey, blob_key)

        pipeline.start()
        return redirect(f'{pipeline.base_path}/status?root={pipeline.pipeline_id}')

app.add_url_rule('/', view_func=IndexHandler.as_view('index'))


def split_into_sentences(s):
  """Split text into list of sentences."""
  s = re.sub(r"\s+", " ", s)
  s = re.sub(r"[\\.\\?\\!]", "\n", s)
  return s.split("\n")


def split_into_words(s):
  """Split a sentence into list of words."""
  s = re.sub(r"\W+", " ", s)
  s = re.sub(r"[_0-9]+", " ", s)
  return s.split()


def word_count_map(data):
  """Word count map function."""
  (entry, text_fn) = data
  text = text_fn()
  if isinstance(text, bytes):
    text = text.decode("utf-8")

  logging.debug("Got %s", entry.filename)
  for s in split_into_sentences(text):
    for w in split_into_words(s.lower()):
      yield (w, "")


def word_count_reduce(key, values):
  """Word count reduce function."""
  yield "%s: %d\n" % (key, len(values))


def index_map(data):
  """Index demo map function."""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Got %s", entry.filename)
  for s in split_into_sentences(text):
    for w in split_into_words(s.lower()):
      yield (w, entry.filename)


def index_reduce(key, values):
  """Index demo reduce function."""
  yield "%s: %s\n" % (key, list(set(values)))


PHRASE_LENGTH = 4


def phrases_map(data):
  """Phrases demo map function."""
  (entry, text_fn) = data
  text = text_fn()
  filename = entry.filename

  logging.debug("Got %s", filename)
  for s in split_into_sentences(text):
    words = split_into_words(s.lower())
    if len(words) < PHRASE_LENGTH:
      yield (":".join(words), filename)
      continue
    for i in range(0, len(words) - PHRASE_LENGTH):
      yield (":".join(words[i:i+PHRASE_LENGTH]), filename)


def phrases_reduce(key, values):
  """Phrases demo reduce function."""
  if len(values) < 10:
    return
  counts = {}
  for filename in values:
    counts[filename] = counts.get(filename, 0) + 1

  words = re.sub(r":", " ", key)
  threshold = len(values) / 2
  for filename, count in counts.items():
    if count > threshold:
      yield "%s:%s\n" % (words, filename)

class WordCountPipeline(base_handler.PipelineBase):
  """A pipeline to run Word count demo.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    output = yield mapreduce_pipeline.MapreducePipeline(
        "word_count",
        "main.word_count_map",
        "main.word_count_reduce",
        "mapreduce.input_readers.BlobstoreZipInputReader",
        "mapreduce.output_writers.GoogleCloudStorageOutputWriter",
        mapper_params={
            "blob_key": blobkey,
            "bucket_name": bucket_name,
        },
        reducer_params={
            "output_writer": {
                "bucket_name": bucket_name,
                "content_type": "text/plain",
            }
        },
        shards=16)
    yield StoreOutput("WordCount", filekey, output)


class IndexPipeline(base_handler.PipelineBase):
  """A pipeline to run Index demo.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """


  def run(self, filekey, blobkey):
    output = yield mapreduce_pipeline.MapreducePipeline(
        "index",
        "main.index_map",
        "main.index_reduce",
        "mapreduce.input_readers.BlobstoreZipInputReader",
        "mapreduce.output_writers.GoogleCloudStorageOutputWriter",
        mapper_params={
            "blob_key": blobkey,
            "bucket_name": bucket_name,
        },
        reducer_params={
            "output_writer": {
                "bucket_name": bucket_name,
                "content_type": "text/plain",
            }
        },
        shards=16)
    yield StoreOutput("Index", filekey, output)


class PhrasesPipeline(base_handler.PipelineBase):
  """A pipeline to run Phrases demo.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, filekey, blobkey):
    output = yield mapreduce_pipeline.MapreducePipeline(
        "phrases",
        "main.phrases_map",
        "main.phrases_reduce",
        "mapreduce.input_readers.BlobstoreZipInputReader",
        "mapreduce.output_writers.GoogleCloudStorageOutputWriter",
        mapper_params={
            "blob_key": blobkey,
            "bucket_name": bucket_name,
        },
        reducer_params={
            "output_writer": {
                "bucket_name": bucket_name,
                "content_type": "text/plain",
            }
        },
        shards=16)
    yield StoreOutput("Phrases", filekey, output)


class StoreOutput(base_handler.PipelineBase):
  """A pipeline to store the result of the MapReduce job in the database.

  Args:
    mr_type: the type of mapreduce job run (e.g., WordCount, Index)
    encoded_key: the DB key corresponding to the metadata of this job
    output: the gcs file path where the output of the job is stored
  """

  def run(self, mr_type, encoded_key, output):
    logging.debug("output is %s" % str(output))
    key = db.Key(encoded=encoded_key)
    m = FileMetadata.get(key)

    blobstore_filename = "/gs" + output[0]
    blobstore_gs_key = blobstore.create_gs_key(blobstore_filename)
    url_path = "/blobstore/" + blobstore_gs_key

    if mr_type == "WordCount":
      m.wordcount_link = url_path
    elif mr_type == "Index":
      m.index_link = url_path
    elif mr_type == "Phrases":
      m.phrases_link = url_path

    m.put()

class UploadHandler(blobstore.BlobstoreUploadHandler):
  """Handler to upload data to blobstore."""

  def post(self):
    source = "uploaded by user"
    upload_files = self.get_uploads(request.environ)
    blob_key = upload_files[0].key()
    name = request.form.get('name')

    user = users.get_current_user()

    username = user.nickname()
    date = datetime.datetime.now()
    str_blob_key = str(blob_key)
    key = FileMetadata.getKeyName(username, date, str_blob_key)

    m = FileMetadata(key_name = key)
    m.owner = user
    m.filename = name
    m.uploadedOn = date
    m.source = source
    m.blobkey = str_blob_key
    m.put()
    
    return redirect(url_for('index'))


@app.route('/upload', methods=['POST'])
def upload():
  return UploadHandler().post()


class DownloadHandler(blobstore.BlobstoreDownloadHandler):
  """Handler to download blob by blobkey."""

  def get(self, key):
    headers = self.send_blob(request.environ, key)
    headers['Content-Type'] = None
    return '', headers


@app.route('/blobstore/<path:key>')
def download(key):
  return DownloadHandler().get(key)

mapreduce_handlers = mapreduce.create_handlers_map("/mapreduce")
for route, handler in mapreduce_handlers:
    app.add_url_rule(route, view_func=handler.as_view(route.lstrip("/")))

if __name__ == '__main__':
  app.run(debug=True)
