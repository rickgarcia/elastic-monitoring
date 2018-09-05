#!/bin/sh
#
# Run as cronjob
# * * * * * /home/kafka/elasticsearch_kafka_cgd/kcgd.sh
#
# Description
#   This collects kafka consumer group description statistics from the cluster
#   and pushes the data to the configured elastic search cluster. At the
#   moment, the kafka bootstrap server and the elasticsearch node are
#   set in the config, and don't have any failover/intelligent cluster-aware
#   balancing. If either node goes down, the collection fails.
# Added 2018/07/31
# By: Rick Garcia <rick.2g@gmail.com>
#
#
# This sh wrapper just ensures the execution occurs in the proper directory
#
cd /home/kafka/elasticsearch_kafka_cgd
python elasticsearch_kcgd.py --config config.json
