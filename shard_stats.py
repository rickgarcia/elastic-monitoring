import sys
import json
import yaml
import datetime
import time
import elasticsearch as es
import threading

# local log configs
from lconfig import *

from pprint import pprint


# Elasticsearch Shard stats index creation
def create_shard_index(es, index_name):
    ss_index_mapping = {
        'mappings': {
            'doc': {
                'dynamic': 'false',
                'properties': {
                    'timestamp': {'type': 'date'},
                    'cluster': {'type': 'keyword'},
                    'node': {
                        'properties': {
                            'name': {'type': 'keyword'},
                            'ip': {'type': 'ip'}
                        }
                    },
                    'index': {
                        'properties': {
                            'name': {'type': 'keyword'},
                            'template': {'type': 'keyword'}
                        }
                    },
                    'shard': {
                        'properties': {
                            'shard_uiid': {'type': 'keyword'},
                            'idx': {'type': 'integer'},
                            'state': {'type': 'keyword'},
                            'shard_type': {'type': 'keyword'},
                            'doc_count': {'type': 'long'},
                            'storage_size': {'type': 'long'}
                        }
                    }
                }
            }
        }
    }

    ss_template = {
        'template': 'shard_stats*',
        'order': 0,
        'aliases': {},
        'settings': {
            'index': {
                'number_of_shards': 1,
                'number_of_replicas': 1
            }
        },
        'mappings': ss_index_mapping['mappings']
    }

    pass



def threaded(fn):
    def wrapper(*k, **kw):
        t = threading.Thread(target=fn, args=k, kwargs=kw)
        t.setDaemon(True)
        t.start()
        return t
    return wrapper


@threaded
def run_shard_query(source_es, dest_es, cluster_name=None, interval=60):

    # offset is in minutes - keep the previous day's suffix for a specified
    # offset after 12:00AM
    def index_partition_suffix(_date = None, offset = 0):
        if (_date is None):
            _date = (datetime.datetime.now() - datetime.timedelta(minutes=offset))
        return '-%04d.%02d.%02d' % (_date.year, _date.month, _date.day)

    log = logging.getLogger(__name__)
    _nextrun = time.time()
    while True:
        # get the index suffixes
        idx_pstr = index_partition_suffix(offset=0)
        idx_pstr2 = index_partition_suffix(offset=-270) # start next day at 22:00

        _st = time.time()
        try:
            es_shardinfo = source_es.cat.shards(
                index='*%s,*%s' % (idx_pstr, idx_pstr2),
                bytes='b',
                v=True,
                format='json')
        except (es.ConnectionError, es.ConnectionTimeout) as e:
            log.warn("connection error %s" % str(e))
            continue
        _se = time.time()
        log.info('%r  %2.2f ms' % ('.cat.shards', (_se - _st) * 1000))

        doc_ts_millis = int(_se * 1000)
        # add the cluster info to each item
        # ... and add in the index prefix
        # - somewhere between the beats and logstash, the date suffix gets
        # attached to the index which is being pushed to the cluster
        #
        #   The switchover isn't terribly consistent - it occurs sometime
        # between 8PM and 12AM depending on how timezoney the server is
        # feeling. This doesn't really create any functional problems, but it
        # causes some gaps in the data where the new indexes start populating
        # their shards. So, to address that, the requesting 'cat' command
        # asks for today's indexes, and the indexes for the day 6 hours
        # ahead. That should be sufficient to cover UTC and CDT zones.
        #
        #   If the target shard for the next day has been populated, then
        # it's added to the list of stats to push to the cluster. If it's
        # from the current day, then we add it to a second list, which is
        # then filtered to remove any shard uiids which have already been
        # entered for the next day
        #
        updated_shard_uuids = [] # shards which have already started reporting the next day
        shard_stats_to_push = []
        current_day_sstats = []
        for _shard in es_shardinfo:

            if (idx_pstr in _shard['index']):
                curr_day = True
                s_template = _shard['index'].replace(idx_pstr, '')
            else:
                curr_day = False
                s_template = _shard['index'].replace(idx_pstr2, '')

            s_uiid = "%03d.%s-%s" % (
                int(_shard['shard']),
                _shard['prirep'],
                s_template)

            sh_data = {
                'timestamp': doc_ts_millis,
                'cluster': cluster_name,
                'node': {
                    'name': _shard['node'],
                    'ip': _shard['ip']
                },
                'index': {
                    'name': _shard['index'],
                    'template': s_template
                },
                'shard': {
                    'shard_type':
                        ('primary', 'replica')[_shard['prirep'] == 'r'],
                    'shard_uiid': s_uiid,
                    'idx': _shard['shard'],
                    'state': _shard['state'],
                    'doc_count': _shard['docs'],
                    'storage_size': _shard['store']
                }
            }
            # push the data to the lists
            if (curr_day):
                current_day_sstats.append(sh_data)
            else:
                shard_stats_to_push.append(sh_data)
                updated_shard_uuids.append(s_uiid)

        # anything in current_dat_sstats that doesn't have an
        # entry in the uiid list is added to the push list
        for s_data in current_day_sstats:
            if (s_data['shard']['shard_uiid'] in updated_shard_uuids):
                pass
            else:
                shard_stats_to_push.append(s_data)

        #
        for sh_data in shard_stats_to_push:
            try:
                dest_es.index(
                    index="test_shard_stats",
                    doc_type="shard_stat",
                    body=json.dumps(sh_data))
            except (es.ConnectionError, es.ConnectionTimeout) as e:
                log.error("Error posting shard %s data %s" %
                    (sh_data['shard']['shard_uiid'], str(e)))
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
        with open('auth.yml') as f:
            es_cred = yaml.load(f)['creds']
    except Exception as e:
        # log error
        raise e

    with open('_clusters.yml') as f:
        clusters = yaml.load(f)

    cluster_threads = []

    for cl_name in clusters.keys():

        voice_es = es.Elasticsearch(
            hosts=clusters[cl_name],
            sniff_on_start=False,
            sniff_on_connection_fail=False,
            http_auth=(es_cred['user'], es_cred['pass']))

        test_es = es.Elasticsearch(
            hosts=[{'host':'testserver', 'port': '9200'}],
            sniff_on_start=False,
            sniff_on_connection_fail=False,
            http_auth=(es_cred['user'], es_cred['pass']))

        log.info("launching %s monitor" % cl_name)
        cluster_threads.append(
            run_shard_query(
                voice_es,
                test_es,
                cluster_name=cl_name,
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
