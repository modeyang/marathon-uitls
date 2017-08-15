# !/usr/bin/env python
# coding=utf-8

import sys
sys.path.append("..")

import logging
from collections import defaultdict, namedtuple

# 3rd party
from kafka import KafkaClient, KafkaConsumer
from kafka.common import OffsetRequestPayload, TopicPartition
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

import config

logger = logging.getLogger("kafka_consumer")
ConsumerLag = namedtuple("ConsumerLag", ["topic", "group", "consumer_lag"])


class KafkaCheck(object):

    def __init__(self, **kwargs):
        self.instances = kwargs.pop("instance", None)
        self.collector = kwargs.pop("collector", None)
        self.zk_timeout = int(kwargs.get('zk_timeout', config.DEFAULT_ZK_TIMEOUT))
        self.kafka_timeout = int(kwargs.get('kafka_timeout', config.DEFAULT_KAFKA_TIMEOUT))

    def read_config(self, instance, key, cast=None):
        if cast and callable(cast):
            return cast(instance.get(key))
        return instance.get(key)

    def _get_all_partitions(self, client, topic):
        return client.get_partition_ids_for_topic(topic)

    def _get_consumer_offset_by_api(self, client, cm_config, servers):
        topic, group_id = cm_config["topic"], cm_config["group"]
        consumer = KafkaConsumer(bootstrap_servers=servers, group_id=group_id)
        consumer.subscribe([topic])
        partitions = self._get_all_partitions(client, topic)
        commited_offsets = dict([(p, consumer.committed(TopicPartition(topic, p))) for p  in partitions])

        # Query Kafka for the broker offsets
        broker_offsets = {}
        offset_responses = client.send_offset_request([
            OffsetRequestPayload(topic, p, -1, 1) for p in partitions])

        for resp in offset_responses:
            broker_offsets[resp.partition] = resp.offsets[0]
        consumer_lags = 0
        for p in partitions:
            consumer_lags += broker_offsets[p] - commited_offsets[p]
        return ConsumerLag(topic, group_id, consumer_lags)

    def _get_offsets_based_on_config(self, client,  zk_conn, zk_prefix, log_name):
        """
        Base the check on what is in the configuration.
        """

        zk_path_partition_tmpl = zk_prefix + '/consumers/%s/offsets/%s/'
        zk_path_offset_tmpl = zk_path_partition_tmpl + '%s'

        consumer_offsets = defaultdict(dict)
        topic, group_id = log_name["topic"], log_name["group"]
        partitions = self._get_all_partitions(client, topic)

        # Remember the topic partitions that we've see so that we can
        # look up their broker offsets later
        for partition in partitions:
            zk_path_offset = zk_path_offset_tmpl % (group_id, topic, partition)
            try:
                offset = int(zk_conn.get(zk_path_offset)[0])
                consumer_offsets[(topic, partition)] = offset
            except NoNodeError:
                logger.warn('No zookeeper node at %s' % zk_path_offset)
            except Exception:
                logger.exception('Could not read consumer offset from %s' % zk_path_offset)

        return consumer_offsets

    def _get_consumer_offset_by_zookeeper(self, zk_hosts, kafka_client, cm_config, zk_prefix=""):
        # Connect to Zookeeper
        zk_conn = KazooClient(zk_hosts, timeout=self.zk_timeout)
        zk_conn.start()

        try:
            consumer_offsets = self._get_offsets_based_on_config(kafka_client, zk_conn, zk_prefix, cm_config)
        finally:
            try:
                zk_conn.stop()
                zk_conn.close()
            except Exception:
                logger.exception('Error cleaning up Zookeeper connection')

        try:
            broker_offsets = {}
            topic, group_id = cm_config["topic"], cm_config["group"]
            partitions = self._get_all_partitions(kafka_client, topic)

            # Query Kafka for the broker offsets
            offset_responses = kafka_client.send_offset_request([
                        OffsetRequestPayload(topic, p, -1, 1) for p in partitions])

            for resp in offset_responses:
                broker_offsets[(resp.topic, resp.partition)] = resp.offsets[0]
        except Exception, e:
            logger.exception(e)

        consumer_lags = 0
        for key in broker_offsets.keys():
            consumer_lags += broker_offsets[key] - consumer_offsets[key]
        return ConsumerLag(topic, group_id, consumer_lags)

    def handle_collector(self, consumer_lags, collector, cluster_name):
        group_topic_lag = defaultdict(list)
        group_topics = defaultdict(list)

        for clag in consumer_lags:
            tags = {'topic': clag.topic, 'consumer_group': clag.group}
            lag = clag.consumer_lag
            logger.info('kafka.consumer_lag.all, %s, %s' % (lag, tags))
            
            # if self.collector: self.collector.add_metric("kafka.consumer_lag.all", cluster_name, lag, tags=tags)
            group_topic_lag[clag.group].append(clag.consumer_lag)
            group_topics[clag.group].append(clag.topic)

        for name, lags in group_topic_lag.iteritems():
            lag = sum(lags)
            tags = {'consumer_group': name, 'topics': "&".join(group_topics[name])}
            logger.info('kafka.consumer_lag.topics.all, %s, %s' % (lag, tags))
            # if self.collector: self.collector.add_metric("kafka.consumer_lag.topics.all", cluster_name, lag, tags=tags)

    def check(self):
        if self.instances is None:
            logger.error("no instances, confirm yaml config")
            return

        [self._check(instance) for instance in self.instances]
        if self.collector:
            self.collector.push()

    def get_topic_consumer_lag(self, consumer_group, topic, zk_enabled=True, zk_prefix=""):
        # Connect to Kafka
        kafka_conn = KafkaClient(config.KAFKA_SERVERS, timeout=self.kafka_timeout)

        # Construct the Zookeeper path pattern
        cm = {"topic": topic, "group": consumer_group, "zk_enabled": zk_enabled}
        if cm.get("zk_enabled", True):
            pcm = self._get_consumer_offset_by_zookeeper(config.KAFKA_ZK, kafka_conn, cm, zk_prefix)
        else:
            pcm = self._get_consumer_offset_by_api(kafka_conn, cm, config.KAFKA_SERVERS)
        print pcm.consumer_lag

    def _check(self, instance):
        """
        Check offset in kafka for consumer_groups,topics and partitions.
        You can ether specify consumer_groups, topics and partitions in
        config file like

        topics:
            - topic: as_nginx-access
              group: as_nginx-access-logstash
              zk_enabled: true(default)
            - topic: as_nginx-access
              group: as_nginx-access-slog-slog
              zk_enabled: false

        """

        zk_connect_str = self.read_config(instance, 'zk_connect_str')
        kafka_host_ports = self.read_config(instance, 'kafka_connect_str')
        cluster_name = self.read_config(instance, "name")

        # Connect to Kafka
        kafka_conn = KafkaClient(kafka_host_ports, timeout=self.kafka_timeout)

        # Construct the Zookeeper path pattern
        zk_prefix = instance.get('zk_prefix', '')
        consumers = self.read_config(instance, 'topics', cast=self._validate_consumer_groups)

        consumer_lags = []
        for cm in consumers:
            if cm.get("zk_enabled", True):
                pcm = self._get_consumer_offset_by_zookeeper(zk_connect_str, kafka_conn, cm, zk_prefix)
            else:
                pcm = self._get_consumer_offset_by_api(kafka_conn, cm, kafka_host_ports)
            consumer_lags.append(pcm)
        
        self.handle_collector(consumer_lags, self.collector, cluster_name)

    def _validate_consumer_groups(self, val):
        """
        Private config validation/marshalling functions
        """

        try:
            for item in val:
                assert isinstance(item, dict)
                assert item.get("topic") is not None and item.get("group") is not None
            return val
        except Exception, e:
            logger.exception(e)
            raise Exception('''The `topics` value must be a mapping of mappings, like this:
                            topics:
                                - topic: as_nginx-access
                                  group: as_nginx-access-logstash
                                  zk_enabled: true(default)
                                - topic: as_nginx-access
                                  group: as_nginx-access-slog-slog
                                  zk_enabled: false
                            ''')


if __name__ == '__main__':
    import os
    import yaml
    agent = KafkaCheck()
    agent.get_topic_consumer_lag("ad_adpc_nginx-logstash", "ad_adpc_nginx", True)
    
