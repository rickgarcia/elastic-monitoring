import sys
import json
import jsondiff
import yaml
import elasticsearch as es
import argparse

# local logging configs
from lconfig import *
# watcher configs
import watchers.watcher_metadata as wm

from pprint import pprint


class es_watcher_config(object):

    def __init__():
        pass

# TODO:
# it would be nice to have this be able to take some command line options
# - write to 'watch.json' template file from cluster
# - use local template file to override cluster
# - target only specific watchers for update/create
# - move watcher metadata configs out of executing file?
def main():

    log = logging.getLogger(__name__)
#    es_cred = None
    try:
        with open('auth.yml') as f:
            es_cred = yaml.load(f)['creds']
        with open('clusters.yml') as f:
            clusters = yaml.load(f)
    except Exception as e:
        # log error
        raise e

    cluster_name = 'monitoring_sat'
    watcher_sets = wm.metadata.keys()

    log.info("available clusters: %s" % repr(clusters.keys()))
    log.info("contacting '%s' cluster" % cluster_name)
    log.info("available watcher configs: %s" % repr(watcher_sets))
    log.debug("cluster nodes: %s" % repr(clusters[cluster_name]))

    # connect to the cluster and create a watcher API object
    prod_es = es.Elasticsearch(
        hosts=clusters[cluster_name],
        sniff_on_start=False,
        sniff_on_connection_fail=False,
        http_auth=(es_cred['user'], es_cred['pass']))
    watch_es = es.client.xpack.watcher.WatcherClient(prod_es)



    with open('watchers/cluster_state/cluster_state.json') as f:
        clst_json = json.load(f)

    try:
        watcher_id = 'cluster_state'
        watch_template = watch_es.get_watch(watcher_id)
        wbody = watch_template['watch']
    except es.exceptions.NotFoundError as e:
        pprint(e)

        wbody = clst_json
        wp_result = watch_es.put_watch(watcher_id, wbody, active=True)
        if (wp_result is not None):
            status = ('Updated', 'Created')[wp_result['created']]
            log.info("%s v%d : %s " % (wp_result['_id'], wp_result['_version'], status))
        else:
            log.info('%s creation failed' % watcher_id)

        sys.exit(0)

    
    
    # pull the template watcher from the cluster
    watch_template = watch_es.get_watch('filesystem_usage')
    watch_body = watch_template['watch']

    with open('watchers/filesystem_usage/filesystem_usage_watcher_template.json') as f:
        fsuw_json = json.load(f)
    #pprint(fsuw_json)

   # jdiff = jsondiff.diff(fsuw_json, watch_body)
    jdiff = jsondiff.diff(watch_body, fsuw_json)

    pprint(jdiff)

    try:
        enodes_sm = watch_es.get_watch('elasticsearch_node_system_memory')
    except es.exceptions.NotFoundError as e:
        pprint(e)
        sys.exit(0)

    pprint(enodes_sm)
    if (True):
        sys.exit(0)

    # clear out the metadata settings for the new watch
    for _w_id in fs_metadata.keys():
        watcher_id = "filesystem_usage_%s" % _w_id
        wbody = watch_template['watch']
        # clear out the metadata from the template and replace with cluster/fs specs
        wbody['metadata'] = {}
        wbody['metadata'] = fs_metadata[_w_id]
        wp_result = watch_es.put_watch(watcher_id, wbody, active=True)
        if (wp_result is not None):
            status = ('Updated', 'Created')[wp_result['created']]
            log.info("%s v%d : %s " % (wp_result['_id'], wp_result['_version'], status))
        else:
            log.info('%s creation failed' % watcher_id)

    # the template watcher gets activated sometimes after being queried;
    # run a disable on it
    watch_es.deactivate_watch('filesystem_usage_alert')

if __name__ == '__main__':
    main()
