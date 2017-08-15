#!/usr/bin/env python
import sys
sys.path.append("..")

import os
import yaml
import logging
import requests
import config
import burrow
import marathon_api
import collections


logger = logging.getLogger(__name__)


class KafkaConsumerReporter(object):
    def __init__(self, url, **kwargs):
        """
        url: burrow api addr
        kwargs: pair of topic and consumer group
        """
        self.kwargs = kwargs
        self.kafka_cluster = kwargs.pop("kafka_cluster", config.KAFKA_CLUSTER)
        self.burrow_client = burrow.BurrowApi(url)

    def consumer_state(self, group, topic=None):
        c_lag = self.burrow_client.consumer_lag(self.kafka_cluster, group)
        kafka_lag = burrow.KafkaConsumerLag(**c_lag)
        return kafka_lag


class MarathonDecider(object):
    def __init__(self, url=config.MARATHON_URI, **kwargs):
        self.marathon_client = marathon_api.MarathonHelper(url, **kwargs)

    def _get_task_count(self, app):
        return len(self.marathon_client.list_tasks(app_id=app))

    def should_scale(self, **kwargs):
        pass

    def scale(self, app, instances=None, delta=2):
        logger.info("scale app: "+ app)
        resp = None
        if instances is not None:
            resp = self.marathon_client.scale_app(app, instances=instances)
        else:
            resp = self.marathon_client.scale_app(app, delta=delta)
        return resp

    def do(self):
        pass


class TopicMaratonDecider(MarathonDecider):

    def __init__(self, topic_cfg, marathon_url=config.MARATHON_URI, burrow_url=config.BURROW_URI, **kwargs):
        self.kafka_reporter = KafkaConsumerReporter(burrow_url)
        self.topic_cfg = topic_cfg
        logger.info(self.topic_cfg)
        super(TopicMaratonDecider, self).__init__(marathon_url, **kwargs)
        
    def should_scale(self, **kwargs):
        cs_lag = kwargs.pop("cs_lag", None)
        if cs_lag is None:
            return False
        if cs_lag.totallag > self.topic_cfg["threshold"] and cs_lag.partition_count > self._get_task_count(self.topic_cfg["app"]): 
            return True
        return False

    def do(self):
        cs_lag = self.kafka_reporter.consumer_state(self.topic_cfg["group"])
        if self.should_scale(cs_lag=cs_lag):
            self.scale(self.topic_cfg["app"])


class DeciderManager(object):
    def __init__(self, **kwargs):
        config_file = kwargs.pop("kafka_config_file", config.KAFKA_CONFIG_FILE)
        kafka_config_path = os.path.join(config.BASE_PATH, "confs/" + config_file)
        cs_config = yaml.load(open(kafka_config_path, "rb"))
        self.deciders = [ TopicMaratonDecider(topic_cfg, username=config.MARATHON_USER, password=config.MARATHON_PASSWD) for topic_cfg in cs_config["topics"]]

    def make_descide(self):
        map(lambda x: x.do(), self.deciders)


if __name__ == "__main__":
    d = DeciderManager()
    d.make_descide()