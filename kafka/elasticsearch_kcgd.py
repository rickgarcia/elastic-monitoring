import sys
import os
import json
import time
import requests
from pprint import pprint, pformat
from threading import active_count as active_thread_count, enumerate as thread_enumerate

import kafka_jmx
import simple_es

import logging
from logging.handlers import RotatingFileHandler

log_level = logging.INFO
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s')
log_file = 'elasticsearch_kafka_cg_desc.log'
log_handler = RotatingFileHandler(
    log_file, 
    mode='a', 
    maxBytes=10*1024*1024,
    backupCount=1,
    encoding=None,
    delay=0)
log_handler.setFormatter(log_formatter)
log_handler.setLevel(log_level)

# internal logger 
log = logging.getLogger(__name__)
log.setLevel(log_level)
log.addHandler(log_handler)


import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", type=str, help="path to json-formatted configuration file")
cmdline_args = parser.parse_args()
if (cmdline_args.config is None):
    cmdline_args.config = 'config.json'


#PUT test_kafka_consumer_groups
index_mapping = {
  "mappings": {
    "doc": {
      "dynamic": "false",
      "properties": {
        "client_id":  { "type": "keyword" },
        "consumer_id":  { "type": "keyword" },
        "current_offset": { "type": "long" },
        "group": { "type": "keyword" },
        "host": { "type": "keyword" },
        "lag": { "type": "integer" },
        "log_end_offset": { "type": "long" },
        "partition": { "type": "integer" },
        "timestamp": { "type": "date" },
        "topic": { "type": "keyword" }
      }
    }
  }
}




# runs the sample processes in parallel
def get_kafka_consumer_group_desc(kcg_cmd, groups):

    kcg_threads = {}
    for group in groups:
        kcg_threads[group] = kcg_cmd.describe_t(group)
    log.debug("kcg java processes initiated - %d dispatch threads:\n%s" % (active_thread_count(), pformat(kcg_threads, indent=4)))

    kcg_desc = {}
    thread_timeout = 120
    for group in groups:
        g_desc = kcg_threads[group].join(thread_timeout)
        if g_desc is not None:
            kcg_desc[group] = g_desc
        else:
            log.warning("kcg request '%s' joined empty" % group)


    if not (active_thread_count() == 1):
        log.warning("%d threads unreturned after timeout (%.2fs)" % ((active_thread_count()), thread_timeout))
        log.debug("Active threads:\n%s" % pformat(thread_enumerate(), indent=4))

    return kcg_desc



# run a single pass of the kafka to elasticsearch pipeline
# es_conn - simple_es connection object
# kfk_cmd - kafka consumer group command object
# kfk_groups - the list of kafka consumer groups to request
#
def run_kf_to_es(es_conn, es_idx, kfk_cmd, kfk_groups):

    _st = time.time()
    group_samples = get_kafka_consumer_group_desc(kfk_cmd, kfk_groups)
    _samplet = time.time()
    log.info("kafka consumer group description response time: %.3f ms" % ((_samplet - _st) * 1000))

    doc_count = 0

    # combine all the group parition descriptions into a single list
    full_payloads = []
    for group_name in group_samples.keys():
        full_payloads = full_payloads + group_samples[group_name]

    try:
        bulk_rc = es_conn.index_bulk(
            index = es_idx, 
            payloads=full_payloads,
            doctype='doc')
        _postt = time.time()

        # error checking for stats
        if (bulk_rc['errors']):
            for doc_status in bulk_rc['items']:
                if not (doc_status['index']['status'] == 201):
                    log.error("error status [%d] in document:\n%s" % (doc_status['index']['status'], pformat(doc_status, indent=4)))
                else:
                    doc_count += 1
        else:
            doc_count = len(bulk_rc['items'])

        log.info("elasticsearch bulk index time (%d documents): %.3f ms" % (doc_count, ((_postt - _samplet) * 1000)))
    except KeyError as e:
        log.error("messed up the document error parsing. dammit. %s" % pformat(e))
        pass
    except Exception as e:
        log.error("Exception in bulk post - dropping errors and continuing: %s " % repr(e))
        pass

    return

#
def main():

    log.info('***********************************')
    log.info("%s - starting" % __file__)
    log.info("using configuration file '%s'", cmdline_args.config)
    #with open('config.json') as config_file:
    with open(cmdline_args.config) as config_file:
        config = json.load(config_file)

    # load the Kafka Consumer Group command object
    log.info("kafka server %s:%d" % (config['kafka_bootstrap_server'], config['kafka_bootstrap_port']))
    kcg_cmd = kafka_jmx.consumer_group_command(
        server = config['kafka_bootstrap_server'],
        port = config['kafka_bootstrap_port'])

    # load up the elastic search object/configs
    log.info("elasticsearch server %s:%d - target index '%s'" % (
        config['elasticsearch_server'], 
        config['elasticsearch_port'], 
        config['elasticsearch_index']))
    es = simple_es.simple_es(
        server=config['elasticsearch_server'],
        port=config['elasticsearch_port'])
    es_index = config['elasticsearch_index']

    # setup the interval
    interval = config['interval']
    if (interval == 0):
        log.info("polling interval is 0; running single pass")
    else:
        log.info("using polling interval %ds" % interval)

    kafka_groups = kcg_cmd.group_list()
    log.info("Kafka group list:\n%s" % pformat(kafka_groups, indent=4))

    _nextrun = time.time()
    while True:
        log.debug("interval expired - initiating dispatch")

        run_kf_to_es(es, es_index, kcg_cmd, kafka_groups)

        if (interval == 0): 
           break
        _nextrun = _nextrun + interval
        # if time.sleep() is called on a negative number, the routine fails
        # to return - so ensure that we're not going to lock up the thread 
        # if one of the preceding calls takes unexpectedly long to return
        # and overruns the interval
        sleep_intv = _nextrun - time.time()
        if (sleep_intv > 0):
            time.sleep(sleep_intv)



if __name__ == '__main__':
    main()
