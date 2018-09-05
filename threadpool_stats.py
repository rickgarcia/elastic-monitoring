import sys
import json
import yaml
import datetime
import time
import requests
import elasticsearch
import regex
import threading

# local log config
from lconfig import *

from pprint import pprint


# Elasticsearch Shard stats index creation:
"""
PUT test_thread_pool_stats
{
  "mappings": {
    "doc": {
      "dynamic": "false",
      "properties": {
        "timestamp": {"type": "date"},
        "thread_id": {"type": "keyword"},
        "node_name": {"type": "keyword"},
        "name": {"type": "keyword"},
        "type": {"type": "keyword"},
        "queue": {"type": "long"},
        "queue_size": {"type": "long"},
        "active": {"type": "integer"},
        "completed": {"type": "long"},
        "rejected": {"type": "long"},
        "largest": {"type": "long"},
        "size": {"type": "long"},
      }
    }
  }
}
"""


# type t The current (*) type of thread pool (fixed or scaling)
# active a The number of active threads in the current thread pool
# size s The number of threads in the current thread pool
# queue q The number of tasks in the queue for the current thread pool
# queue_size qs The maximum number of tasks permitted in the queue for the current thread pool
# rejected r The number of tasks rejected by the thread pool executor
# largest l The highest number of active threads in the current thread pool
# completed c The number of tasks completed by the thread pool executor
# min mi The configured minimum number of active threads allowed in the current thread pool
# max ma The configured maximum number of active threads allowed in the current thread pool
# keep_alive k The configured keep alive time for threads
# node_id id The unique node ID
# ephemeral_id eid The ephemeral node ID
# pid p The process ID of the running node
# host h The hostname for the current node
# ip i The IP address for the current node
# port po The bound transport port for the current node


def threaded(fn):
    def wrapper(*k, **kw):
        t = threading.Thread(target=fn, args=k, kwargs=kw)
        t.setDaemon(True)
        t.start()
        return t
    return wrapper

@threaded
def run_stat_query(source_es, dest_es, cluster_name=None, interval=60):

    log = logging.getLogger(__name__)
    _nextrun = time.time()
    while True:

        _st = time.time()
        try:
            es_statinfo = source_es.cat.thread_pool(
                h='name,node_name,host,type,active,size,queue,queue_size,rejected,largest,completed',
                v=True,
                format='json')
        except (elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout) as e:
            log.warn("connection error %s" % str(e))
            continue
        _se = time.time()
        log.info('%r  %2.2f ms' % ('.cat.thread_pool', (_se - _st) * 1000))

        doc_ts_millis = int(_se * 1000)

       # pprint (es_shardinfo)
       # sys.exit(0)
        # add the cluster info to each item
        # ... and add in the index prefix
        for _stat in es_statinfo:
            stat_data = {
                'timestamp': doc_ts_millis,
                'thread_id': "%s_%s" % (_stat['name'], _stat['node_name']),
                'node_name': _stat['node_name'],
                'name': _stat['name'],
                'type': _stat['type'],
                'queue': _stat['queue'],
                'queue_size': _stat['queue_size'],
                'active': _stat['active'],
                'completed': _stat['completed'],
                'rejected': _stat['rejected'],
                'largest': _stat['largest'],
                'size': _stat['size']
            }

            try:
                dest_es.index(
                    index="test_thread_pool_stats",
                    doc_type='thread_pool',
                    body=json.dumps(stat_data))
            except (elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout) as e:
                log.error("Error posting thread pool %s data %s" %
                    (stat_data['thread_id'], str(e)))
                pass

        _nextrun = _nextrun + interval
        # if time.sleep() is called on a negative number, the routine fails
        # to return - so ensure that we're not going to lock up the thread
        # if one of the preceding calls takes unexpectedly long to return
        # and overruns the interval
        sleep_intv = _nextrun - time.time()
        if (sleep_intv > 0):
            time.sleep(sleep_intv)


def main():

    log = logging.getLogger(__name__)
    es_cred = None
    try:
#        with open('auth.json') as f:
#            es_cred = json.load(f)
        with open('auth.yml') as f:
            es_cred = yaml.load(f)['creds']
    except Exception as e:
        # log error
        raise e

    with open('_clusters.yml') as f:
        clusters = yaml.load(f)

    cluster_threads = []

    for cluster_name in clusters.keys():

        es = elasticsearch.Elasticsearch(
            hosts=clusters[cluster_name],
            sniff_on_start=False,
            sniff_on_connection_fail=False,
            http_auth=(es_cred['user'], es_cred['pass']))

        test_es = elasticsearch.Elasticsearch(
            hosts=[{'host':'localhost', 'port': '9200'}],
            sniff_on_start=False,
            sniff_on_connection_fail=False,
            http_auth=(es_cred['user'], es_cred['pass']))

        log.info("launching %s monitor" % cluster_name)
        cluster_threads.append(
            run_stat_query(
                es,
                test_es,
                cluster_name=cluster_name,
                interval=30))

    # Thread startup is done - monitor the active threads
    log.info("Threads: %s", repr(cluster_threads))
    while True:
        log.debug("main thread check-in: %d worker threads active" %
            (threading.active_count() - 1))
        threads_active = 0
        for c_thread in cluster_threads:
            c_thread.join(1)
        if (threading.active_count() == 1):
            # TODO: restart any threads that have died
            break
        time.sleep(15)

if __name__ == '__main__':
    main()
