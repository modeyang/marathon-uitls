# !/usr/bin/env python
# coding=utf-8

import os
import re
import json
import yaml
import logging
import collections
import config
from api.marathon_api import MarathonHelper
from utils.zk_util import ZKHelper

logger = logging.getLogger(__file__)


class LogstashMonitor(object):
    ip_pattern = re.compile(".*bjyg-(\d+-\d+-\d+-\d+).*", re.I)

    def __init__(self, config_file=config.KAFKA_CONFIG_FILE, **kwargs):
        config_file = kwargs.pop("kafka_config_file", config.KAFKA_CONFIG_FILE)
        kafka_config_path = os.path.join(config.BASE_PATH, "confs/" + config_file)
        self.cs_config = yaml.load(open(kafka_config_path, "rb"))
        self.marathon_client = MarathonHelper(config.MARATHON_URI, username=config.MARATHON_USER, password=config.MARATHON_PASSWD)
        self.zk_helper = ZKHelper(config.KAFKA_ZK) 

    def _extract_host(self, hostname):
        m = self.ip_pattern.search(hostname)
        if m is None:
            logger.error("regex hostname: %s failed, pattern:%s" % (hostname, self.ip_pattern.pattern))
            return None
        return m.group(1)

    def _check_lack_hosts(self, zk_cs_host, task_hosts):
        lack_hosts = []
        for host, count in task_hosts.iteritems():
            if host not in zk_cs_host:
                lack_hosts.append(host)
                continue
            if count > zk_cs_host[host]:
                lack_hosts.append(host)
        return lack_hosts

    def _check_topic(self, topic_cfg):
        logger.info(topic_cfg)
        zk_consumers = self.zk_helper.get_consumers(topic_cfg["group"])
        consumers = [self._extract_host(cs) for cs in zk_consumers]
        consumers = filter(lambda x: x is not None, consumers)

        tasks = self.marathon_client.list_tasks(topic_cfg["app"])
        host_task_dict = collections.defaultdict(list)
        map(lambda x: host_task_dict[x.host].append(x.id), tasks)

        lack_hosts = self._check_lack_hosts(collections.Counter(consumers), collections.Counter([t.host for t in tasks ]))
        kill_tasks = []
        map(lambda h: kill_tasks + host_task_dict[h], lack_hosts)

        logger.info("need kill tasks %s" % kill_tasks )
        self.marathon_client.kill_given_tasks(kill_tasks)

    def check(self):
        for topic_cfg in self.cs_config["topics"]:
            self._check_topic(topic_cfg)
