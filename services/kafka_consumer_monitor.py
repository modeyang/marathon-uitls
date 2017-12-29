# !/usr/bin/env python
# coding=utf-8

import sys
sys.path.append("..")

import logging
import time
import pprint

from api.burrow import BurrowApi
from utils.zk_util import ZKHelper
from utils.kafka_consumer_v2 import KafkaCheck
from utils.influx_util import InfluxHelper

import config

logger = logging.getLogger("kafka_consumer")


class KafkaConsumerMonitor(object):
    def __init__(self, *args):
        super(KafkaConsumerMonitor, self).__init__(*args) 
        self.burrow = BurrowApi(config.BURROW_URI)
        self.zk_helper = ZKHelper(config.KAFKA_ZK) 
        self.kafka_helper = KafkaCheck(zk_client=self.zk_helper)
        self.influx_helper = InfluxHelper("kafka")

    def get_avaliable_consumers(self, delete=False):
        groups = []
        path = "/consumers"
        consumers = self.zk_helper.get_children(path)

        for grp in consumers:
            try:
                c_lag = self.burrow.consumer_lag_obj(config.KAFKA_CLUSTER, grp)
                if c_lag.complete: groups.append(c_lag)
            except Exception as e:
                logger.error("%s catch error: %s" % (grp, e))
                if delete:
                    del_path = path + "/" + grp
                    self.zk_helper.delete(del_path, recursive=True)
        return groups

    def get_avaliable_consumers_by_api(self, group=None):
        groups = []
        consumers = self.burrow.consumers(config.KAFKA_CLUSTER)
        for grp in consumers:
            try:
                c_lag = self.burrow.consumer_lag_obj(config.KAFKA_CLUSTER, grp)
                if c_lag.complete: groups.append(c_lag)
            except Exception as e:
                logger.error("%s catch error: %s" % (grp, e))
        return groups

    def check(self):
        metric_dict = {}
        use_grps = self.get_avaliable_consumers_by_api()
        zk_consumers = self.zk_helper.get_children("/consumers")
        metric = "kafka.consumerLag"
        threads = []
        for clag in use_grps:
            if all([(p["status"] == "STOP") for p in clag.partitions ]):
                logger.info("error consumer group: %s, skip it" % clag.group)
                continue
            grp = clag.group
            topics = self.burrow.consumer_topics(config.KAFKA_CLUSTER, grp)
            if grp in zk_consumers and any([(p["status"] == "ERR") for p in clag.partitions]):
                for tp in topics:
                    lag = self.kafka_helper.get_topic_consumer_lag(grp, tp)
                    counter = metric + "/topic={0},group={1}".format(tp, grp)
                    metric_dict[counter] = lag
            else:
                if len(topics) == 1:
                    lag = clag.totallag
                    counter = metric + "/topic={0},group={1}".format(topics[0], grp)
                    metric_dict[counter] = lag
                else:
                    for tp in topics:
                        counter = metric + "/topic={0},group={1}".format(tp, grp)
                        tp_partitions = [ p for p in clag.partitions if p["topic"] == tp ]
                        if all([(p["status"] == "STOP") for p in tp_partitions ]):
                            logger.info("error consumer group: %s, topic: %s, skip it" % (grp, tp))
                            continue
                        lag = sum([ p["end"]["lag"] for p in tp_partitions ])
                        metric_dict[counter] = lag

        logger.info("push metrics to influxdb, size:%s" % len(metric_dict))
        self.influx_helper.bulk_metrics(metric_dict, int(time.time()))

    def _grp_topic_lag(self, group, topic):
        metric_dict = {}
        topic_lag = self.burrow.consumer_lag_obj(config.KAFKA_CLUSTER, group, topic)
        return topic_lag.totallag


if __name__ == '__main__':
    # print KafkaConsumerMonitor()._grp_topic_lag("LIUJIAN_LOGMAN", "ad_adserver")
    KafkaConsumerMonitor().check()

