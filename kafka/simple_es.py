import sys
import os
import json
import time
import requests
import logging
from pprint import pformat

log = logging.getLogger(__name__)

# The main and entire point of this module is to provide some very basic
# elasticsearch operations on servers/systems where installing the full
# elasticsearch-py module might not be permitted, or simply be more trouble
# than rewriting a few post/gets for very basic elasticsearch use cases
#
# Consequently, this class has the methods which have been required in the
# past, and nothing more.
#
# TODO: almost everything of any complexity. If it's something that will
#       take more than a day or two to implement, then find a way to get
#       approval to use the "official" elasticsearch-py package instead
#
# simple_es
class simple_es(object):

    def index_post(self, index=None, payload=None, doctype='doc'):
        url = "%s/%s/doc/" % (self._url, index)
        js_payload = json.dumps(payload)
        log.debug("index doc (%d bytes) %s" % (len(js_payload), url))
        try:
            idx_post = requests.post(url, data=js_payload, auth=self._auth)
        except ConnectionError as e:
            log.debug("status code: %d" % idx_post.status_code)
            raise e
        return json.loads(idx_post.content)

    # bulk post format - each document must be preceded by an "action" line,
    # specifying the index and document type
    # each action line and document must be delimited by a newline
    def index_bulk(self, index=None, payloads=None, doctype='doc'):

        url = "%s/_bulk" % (self._url)
        log.debug("bulk index (%d docs) %s" % (len(payloads), url))

        # action lines can include _index, _type, and id - leave _id out to
        # force it to automatically create the document id
        action_line = '{"index": { "_index": "%s", "_type": "%s" } }' % (index, doctype)
        payload_lines = []
        for payload in payloads:
            payload_lines.append(action_line)
            payload_lines.append(json.dumps(payload))
        # each actionline and payload must be newline deliminated
        bulk_payload = "\n".join(payload_lines)

        snippet_len = (1024, len(bulk_payload))[len(bulk_payload) < 1024]
        log.debug("bulk payload formatted:\n\n%s\n...\n" % bulk_payload[:snippet_len])
        try:
           bulk_post = requests.post(url, data=bulk_payload, auth=self._auth)
        except ConnectionError as e:
            log.debug("status code: %d" %bulk_post.status_code)
            raise e
        return json.loads(bulk_post.content)

    def index_get(self, index=None):
        idx_get_url = "%s/%s" % (self._url, index)
        idx_get = requests.get(idx_get_url, auth=self._auth)
        return json.loads(idx_get.content)

    def set_auth(self, user=None, password=None):
        self._auth = (user, password)

    def load_auth_file(self, filename):
       with open(filename) as f:
           es_cred = json.load(f)
       try:
           self._auth = (es_cred['user'], es_cred['pass'])
       except KeyError as e:
           log.warning("Error in auth file: %s" % pformat(e, indent=4))
           pass

    def info(self):
        info_get = requests.get(self._url, auth=self._auth)
        return json.loads(info_get.content)

    def __init__(self, server = None, auth = None, port = 9200):
        if (server is None):
            self.server = os.uname()[1]
        else:
            self._server = server
        self._port = port
        # do http/https here
        self._url = "http://%s:%d" % (self._server, self._port)
        self._auth = auth


#
def main():

    logging.basicConfig(level=logging.DEBUG)

    log.debug("starting %s" % __file__)
    server_name = os.uname()[1]
    es = simple_es(server=server_name)
    es_index = 'test_kafka_consumer_groups'

    idx_get = es.index_get(es_index)
    log.info("Index request:\n%s" % pformat(idx_get, indent=4))


if __name__ == '__main__':
    main()
