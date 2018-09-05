import sys
import json
import yaml
import datetime
import time
import requests
import elasticsearch
import regex
import threading

import logging
import logging.config

from pprint import pprint


###
log_config = {
    'disable_existing_loggers': True,
    'version': 1,
    'formatters': {
        'short': {
            'format': '%(asctime)s %(levelname)s %(name)s: %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'formatter': 'short',
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'INFO',
        }
    }
}

logging.config.dictConfig(log_config)


# Elasticsearch Shard stats index creation:
"""
PUT test_status_count_summary
{
  "mappings": {
    "doc": {
      "dynamic": "false",
      "properties": {
        "status": {"type": "keyword" },
        "@timestamp": {"type": "date" },
        "interval": {"type": "keyword" },
        "doc_count": {"type": long },
        "doc_count_total": {"type": long },
        "beat.name": {"type": "keyword" },
        "pipeline.kafka_topic_id": {"type": "keyword" },
        "pipeline.egress.hostname": {"type": "keyword" },
        "pipeline.ingress.hostname": {"type": "keyword" },
        "fields.team": {"type": "keyword" }
      }
    }
  }
}
"""

index_mappings = {
  "mappings": {
    "doc": {
      "dynamic": "false",
      "properties": {
        "status": {"type": "keyword" },
        "@timestamp": {"type": "date" },
        "interval": {"type": "keyword" },
        "doc_count": {"type": long },
        "doc_count_total": {"type": long },
      }
    }
  }
}

agg_keys = [
    'beat.name', 'pipeline.kafka_topic_id',
    'pipeline.egress.hostname', 'pipeline.ingress.hostname',
    'fields.team']

interval_names = {
    '1m':  60,
    '5m':  (60 * 5),
    '15m': (60 * 15),
    '1h':  (60 * 60),
    '24h': (60 * 60 * 24),
    '1d':  (60 * 60 * 24)
}

# The reindexing is intended to produce a list of documents primarily
# indexed by status, and then secondarily by the various sub-buckets
# ?should this def be called reindex or flatten?
def reindex_aggregation(agg_response):

    logger = logging.getLogger(__name__)
    if (agg_response['timed_out']):
        logger.error("Timeout found in aggregation response")
        return None

    try:
        status_buckets = agg_response['aggregations']['status']['buckets']
    except KeyError as e:
        logger.error("response format unexpected")
        return None

    # Start extracting from the nested aggs and start flattening
    reindexed_docs = []
    for s_bucket in status_buckets:
        try:
            reindexed_docs.append({
                'status': s_bucket['key'],
                '@timestamp': agg_response['interval_timestamp'],
                'doc_count_total': s_bucket['doc_count'],
                'interval': agg_response['interval']
            })
        except KeyError as e:
            logger.error("required fields missing in response %s" % repr(e))
            return None

        for agg_key in agg_keys:
            if (agg_key in s_bucket):
                subagg_buckets = s_bucket[agg_key]['buckets']
                for _sab in subagg_buckets:
                    reindexed_docs.append({
                        'status': s_bucket['key'],
                        '@timestamp': agg_response['interval_timestamp'],
                        'interval': agg_response['interval'],
                        agg_key: _sab['key'],
                        'doc_count': _sab['doc_count']
                    })

    return(reindexed_docs)


def gen_subagg_query(primary, secondaries):

    q_template = {
        "query": {
            "range": {
                "@timestamp": {
                    # default five minutes
                    "gte": long((time.time() - 300) * 1000),
                    "lte": long(time.time() * 1000),
                    "format": "epoch_millis"
                }
            }
        },
        "aggs": {
            ("%s" % primary): {
                "terms": {
                    "field": primary
                }
            }
        },
        "size": 0
    }

    # setup the secondary aggregations
    q_template['aggs'][primary]['aggs'] = {}
    for _agg in secondaries:
        q_template['aggs'][primary]['aggs'][_agg] = {
            "terms": { "field": _agg }
        }

    return (q_template)


def main():

    logger = logging.getLogger(__name__)
    es_cred = None
    try:
        with open('auth.yml') as f:
            es_cred = yaml.load(f)['creds']
    except Exception as e:
        # log error
        raise e

    with open('clusters.yml') as f:
        clusters = yaml.load(f)

    logging.info("contacting cluster...")
    test_es = elasticsearch.Elasticsearch(
        hosts=[{'host': 'localhost', 'port': '9200'}],
        sniff_on_start=False,
        sniff_on_connection_fail=False,
        http_auth=(es_cred['user'], es_cred['pass']))



    interval_duration = '15m'
    # timestamps are still in seconds at this point...
    _set_ts = long(time.time())
    interval_end = _set_ts - (_set_ts % interval_names[interval_duration])
    interval_start = interval_end - interval_names[interval_duration]
    # convert to milliseconds
    interval_end = interval_end * 1000
    interval_start = interval_start * 1000
    interval_delta = interval_names[interval_duration] * 1000

    run_thirty_days = int(60/15) * 24 * 30

    # generate the aggregation query
    agg_query_json = gen_subagg_query('status', agg_keys)
    for i in range(run_thirty_days):
        interval_end = interval_end - interval_delta
        interval_start = interval_start - interval_delta

        agg_query_json['query']['range']['@timestamp']['gte'] = interval_start
        agg_query_json['query']['range']['@timestamp']['lte'] = interval_end

        # submit the query and add the timestamp info to the response
        logger.debug("submitting query: %s" %
            json.dumps(agg_query_json, indent=4))
        try:
            ridx_aggs = test_es.search(index='filebeat-*', body=agg_query_json)
        except (elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout) as e:
            logger.error("could not complete query %s", repr(e))
            continue

        ridx_aggs['interval'] = interval_duration
        ridx_aggs['interval_timestamp'] = interval_end

        doc_list = reindex_aggregation(ridx_aggs)

        for doc in doc_list:
            try:
                test_es.index(
                    index='test_status_count_summary',
                    doc_type='doc',
                    body=json.dumps(doc))
            except (elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout) as e:
                logger.error("could not complete query %s", repr(e))
                continue

        print ("ran iteration %d" % i)

    sys.exit(0)

if __name__ == '__main__':
    main()
