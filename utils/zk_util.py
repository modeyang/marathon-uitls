# !/usr/bin/env python
# coding=utf-8
import time
import logging
import config

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.client import KazooState
from lib_util import APIProxy

logger = logging.getLogger(__file__) 

class ZKHelper(object):

    def __init__(self, zk_hosts, **kwargs):
        zk_timeout = int(kwargs.get('zk_timeout', config.DEFAULT_ZK_TIMEOUT))
        self.zk_client = KazooClient(zk_hosts, timeout=zk_timeout)
        self.proxy = APIProxy(self.zk_client)
        self.zk_client.add_listener(self.listener)
        self.zk_client.start()
        
    @property
    def zkClient(self):
        return self.zk_client

    def stop(self):
        self.zk_client.stop()
        self.zk_client.close()

    def listener(self, state):
        pass

    def do(self):
        while True:
            time.sleep(2)

    def __del__(self):
        self.stop()

    def __getattr__(self, name):
        return getattr(self.proxy, name)

    def get_consumer_ids(self, group):
        path = "/consumers/%s/ids" % group
        return self.zk_client.get_children(path)

    def get_consumers(self, group, topic):
        path = "/consumers/%s/owners/%s" % (group, topic)
        ids = self.zk_client.get_children(path)
        consumers = [ self.zk_client.get(path + "/" + p)[0] for p in ids]
        return list(set(consumers))

    def remove_unuse_logstash_consumer(self):
        consumer_path = "/consumers/"
        consumers = self.zk_client.get_children(consumer_path)
        for cs in consumers:
            id_path = consumer_path + "%s/ids/" % cs
            try:
                ids = self.zk_client.get_children(id_path)
                if len(ids) == 0:
                    logger.info("need to removed consumer: " + cs)
                    cs_path = consumer_path + cs
                    self.zk_client.delete(cs_path, recursive=True)
            except NoNodeError, e:
                logger.info("no ids in path: " + id_path)
                pass
            except Exception, e:
                logger.error(e)

    def remove_invalid_consumers(self):
        consumer_path = "/consumers/"
        consumers = self.zk_client.get_children(consumer_path)
        for cs in consumers:
            id_path = consumer_path + "%s/ids/" % cs
            try:
                ids = self.zk_client.get_children(id_path)
            except NoNodeError, e:
                logger.info("no ids in path: " + id_path + ", remove it")
                self.zk_client.delete(consumer_path + cs, recursive=True)
            except Exception, e:
                logger.error(e)

