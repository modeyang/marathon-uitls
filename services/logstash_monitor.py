# !/usr/bin/env python
# coding=utf-8

import os
import re
import json
import yaml
import time
import logging
import collections
import config
from api.marathon_api import MarathonHelper
from api.burrow import BurrowApi
from utils.zk_util import ZKHelper

logger = logging.getLogger(__file__)


class LogstashMonitor(object):
    ip_pattern = re.compile(".*bjyg-(\d+-\d+-\d+-\d+).*", re.I)

    def __init__(self, config_file=config.KAFKA_CONFIG_FILE, **kwargs):
        config_file = kwargs.pop("kafka_config_file", config.KAFKA_CONFIG_FILE)
        kafka_config_path = os.path.join(config.BASE_PATH, "confs/" + config_file)
        self.cs_config = yaml.load(open(kafka_config_path, "rb"))
        self.kafka_cluster = self.cs_config["cluster"]
        self.marathon_client = MarathonHelper(config.MARATHON_URI, username=config.MARATHON_USER, password=config.MARATHON_PASSWD)
        self.burrow_client = BurrowApi(config.BURROW_URI)
        self.zk_helper = ZKHelper(config.KAFKA_ZK)

    def _extract_host(self, hostname):
        m = self.ip_pattern.search(hostname)
        if m is None:
            logger.error("regex hostname: %s failed, pattern:%s" % (hostname, self.ip_pattern.pattern))
            return None
        ip = m.group(1)
        return ip.replace("-", ".")

    def _check_lack_hosts(self, zk_cs_host, task_hosts):
        lack_hosts = []
        for host, count in task_hosts.iteritems():
            if host not in zk_cs_host:
                lack_hosts.append(host)
                continue
            if count > zk_cs_host[host]:
                lack_hosts.append(host)
        return lack_hosts

    def _get_zk_consumers(self, topic_cfg):
        index = 3
        while index > 0:
            try:
                zk_consumers = self.zk_helper.get_consumers(topic_cfg["group"], topic_cfg["topic"])
                return zk_consumers
            except Exception, e:
                time.sleep(1)
            index -= 1
        return None

    def _check_topic(self, topic_cfg):
        logger.info(topic_cfg)
        logger.info(json.dumps(self.burrow_client.consumer_lag_json(self.kafka_cluster, topic_cfg["group"]), indent=2))

        zk_consumers = self._get_zk_consumers(topic_cfg)
        if zk_consumers is None:
            return
        consumers = [self._extract_host(cs) for cs in zk_consumers]
        consumers = filter(lambda x: x is not None, consumers)

        tasks = self.marathon_client.list_tasks(topic_cfg["app"])
        host_task_dict = collections.defaultdict(list)
        map(lambda x: host_task_dict[x.host].append(x.id), tasks)

        lack_hosts = self._check_lack_hosts(collections.Counter(consumers), collections.Counter([t.host for t in tasks ]))
        kill_tasks = []
        for h in lack_hosts:
            kill_tasks += host_task_dict[h]

        logger.info("need kill tasks %s" % kill_tasks )
        return self.marathon_client.kill_given_tasks(kill_tasks, force=True)

    def check(self):
        for topic_cfg in self.cs_config["topics"]:
            self._check_topic(topic_cfg)
