elastic-monitoring

- scripts/programs for internally monitoring elasticsearch

- shard_stats.py - collects and indexes shard statistics from the _cat interface 
    Queries the target cluster and forwards shard statistics to the monitoring cluster; the shard statistics are collected and grouped using an index prefix (ie, filebeat for filebeat-2018.07...). This enables the shards for related indexes to be graphed on a daily basis to monitor shard size, behavior, and the corresponding index health.

- threadpool_stats.py - collects and indexes threadpool statistics from the _cat interface
    Queries threadpool statistics from the target cluster and forwards the stats to the monitoring cluster.

- watcher_setup.py - installs a set of watchers based on a template in a cluster. 
    Watchers in this set are written to be configurable by requiring only the updating of the metadata section of the watcher to target different clusters, nodes, or other resources
    - filesystem_usage_* - can be configured with the following variables:
    {
        'cluster_name': 'example_cluster',
        'filesystem': '/opt/elastic',
        'name': 'Watcher Description',
        'threshold': 0.9,
        'window_period': '5m'
    }
    an example of the list of generated watchers is located in watchers/watcher_metadata.py


