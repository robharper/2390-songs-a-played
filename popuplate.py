from tornado.httpclient import HTTPClient, HTTPRequest, HTTPError
from tornado.ioloop import IOLoop
import tornado.options
import json
import time
import calendar
import glob
import sys
from os.path import join
import hashlib

DEFAULT_ES_URL = "http://localhost:19200"
DEFAULT_INDEX_NAME = "xmas"

http_client = HTTPClient()


# ------
def ingest (datapath):
  to_load = glob.glob(datapath+"/*.json")
  for data_file in to_load:
    data = json.load(open(data_file))
    for song in data:
      song['play_id'] = hashlib.md5(str(song['updated_at']) + '_' + str(song['started_at'])).hexdigest()
      song['updated_at'] = int(song['updated_at']) * 1000
    upload_batch(data)
      
# ------
def create_index():
  schema = {
    "settings": {
        "number_of_shards": 1
    },
    "mappings": {
      "song": {
        "properties": {
          "radio_station": {"type": "keyword"},
          "artist": {"type": "keyword"},
          "album": {"type": "keyword"},
          "song_title": {"type": "keyword"},
          "art_url": {"type": "keyword"},
          "high_res_art_url": {"type": "keyword"},
          "itunes_purchase_url": {"type": "keyword"},
          "started_at": {"type": "keyword"},
          "updated_at": {"type": "date"},
          "spotify": {"type": "keyword"}
        }
      }
    }
  }

  body = json.dumps(schema)
  url = "%s/%s" % (tornado.options.options.es_url, tornado.options.options.index_name)
  headers = {
    'Content-Type': 'application/json'
  }
  try:
    request = HTTPRequest(url, method="PUT", headers=headers, body=body, request_timeout=240)
    response = http_client.fetch(request)
    print('Create index done   %s' % response.body)
  except HTTPError as err:
    print 'Failed to create index', err

# ------
def delete_index():
  try:
    url = "%s/%s" % (tornado.options.options.es_url, tornado.options.options.index_name)
    headers = {
      'Content-Type': 'application/json'
    }
    request = HTTPRequest(url, method="DELETE", headers=headers, request_timeout=240)
    response = http_client.fetch(request)
    print('Delete index done   %s' % response.body)
  except HTTPError as err:
    print 'Failed to delete index', err

# ------
total_uploaded = 0
def upload_batch(upload_data):
  upload_data_txt = ""
  for item in upload_data:
    cmd = {'index': {'_index': tornado.options.options.index_name, '_type': 'song', '_id': item['play_id']}}
    upload_data_txt += json.dumps(cmd) + "\n"
    upload_data_txt += json.dumps(item) + "\n"

  headers = {
    'Content-Type': 'application/x-ndjson'
  }

  request = HTTPRequest(tornado.options.options.es_url + "/_bulk", method="POST", headers=headers, body=upload_data_txt, request_timeout=240)
  response = http_client.fetch(request)
  result = json.loads(response.body)

  global total_uploaded
  total_uploaded += len(upload_data)
  res_txt = "OK" if not result['errors'] else "FAILED"
  print("Upload: %s - upload took: %4dms, total messages uploaded: %6d" % (res_txt, result['took'], total_uploaded))


# ------
if __name__ == '__main__':
  tornado.options.define("es_url", type=str, default=DEFAULT_ES_URL,
                           help="URL of your Elasticsearch node")

  tornado.options.define("index_name", type=str, default=DEFAULT_INDEX_NAME,
                          help="Name of the index to store your messages")

  tornado.options.define("indir", type=str, default="./data",
                          help="The path to the input files")

  tornado.options.define("init", type=bool, default=False,
                          help="Force deleting and re-initializing the Elasticsearch index")

  tornado.options.parse_command_line()

  if tornado.options.options.indir:
    if tornado.options.options.init:
      delete_index()
    create_index()
    ingest(tornado.options.options.indir)
  else:
    tornado.options.print_help()