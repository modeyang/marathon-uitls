# !/usr/bin/env python
# coding=utf-8

import os
import re
import json
import yaml
import logging
import collections
import config
from config import MARATHON_USER, MARATHON_PASSWD, DOCKER_URL
from api.marathon_api import MarathonHelper
from api.docker_api import DockerHelper
from api.mesos_api import MesosHelper

logger = logging.getLogger(__file__)


class MesosAgentDocker(object):
    AGENT_CACHE = {}

    def __init__(self, hostname, *args, **kwargs):
        self.hostname = hostname
        self.docker_url = kwargs.pop("docker_url", DOCKER_URL)
        self.docker_label_filter = kwargs or {"MESOS_TASK_ID": "logstash"}

    @classmethod
    def get_docker_client(cls, docker_url, hostname, **kwargs):
        if "timeout" not in kwargs: 
            kwargs["timeout"] = config.DOCKER_TIMEOUT 

        docker_client = None
        if hostname in cls.AGENT_CACHE:
            docker_client = cls.AGENT_CACHE[hostname]
            try:
                docker_client.ping()
            except:
                docker_client = DockerHelper(docker_url, **kwargs)
                cls.AGENT_CACHE[hostname] = docker_client
        else:
            docker_client = DockerHelper(docker_url, **kwargs)
            cls.AGENT_CACHE[hostname] = docker_client
        return docker_client

    def _filter_field(self, _ps_dict):
        for key, value in self.docker_label_filter.iteritems():
            if key not in _ps_dict:
                return False
            else:
                if value not in _ps_dict[key]:
                    return False
        return True

    def _agent_docker_ps(self):
        # must set tcp url in docker startup
        docker_client = self.get_docker_client(self.docker_url, self.hostname)
        _ps_list = [docker_client.ps_info(c) for c in docker_client.containers.list() ]
        return [p for p in _ps_list if self._filter_field(p)]

    def container_blkio_stats(self, cid):
        docker_client = self.get_docker_client(self.docker_url, self.hostname)
        container = docker_client.containers.get(cid)
        r = container.stats(stream=False)["blkio_stats"]["io_service_bytes_recursive"]
        return max([f["value"] for f in r if f["op"] == "Read"])

    def is_blkio_trigger(self, value):
        return value / 1024.0 / 1024 / 1024 > 15

    def stop_container(self, cid):
        docker_client = self.get_docker_client(self.docker_url, self.hostname)
        container = docker_client.containers.get(cid)
        return container.stop()

    def monitor(self):
        for ci in self._agent_docker_ps():
            blkio_value = self.container_blkio_stats(ci["id"])
            if self.is_blkio_trigger(blkio_value):
                print ci["id"], ci["MESOS_TASK_ID"]
                self.stop_container(ci["id"])

    def __repr__(self):
        docker_client = self.get_docker_client(self.docker_url, self.hostname)
        resp = {"hostname": self.hostname}    
        if docker_client is not None:
            resp = resp.update(docker_client.info())
        return json.dumps(resp, indent=2)


class MesosDocker(object):
    AGENT_CACHE = {}

    def __init__(self, *args, **kwargs):
        self.mesos_client = MesosHelper() 
        self.marathon_client = MarathonHelper(username=MARATHON_USER, password=MARATHON_PASSWD)

    def mesos_agents(self):
        return self.mesos_client.agents()

    def get_docker_client(self, hostname):
        docker_client = None
        if hostname in self.AGENT_CACHE:
            docker_client = self.AGENT_CACHE[hostname]
        else:
            url = "tcp://" + hostname + ":2375"
            docker_client = DockerHelper(url)
            self.AGENT_CACHE[hostname] = docker_client
        return docker_client

    def _agent_docker_ps(self, hostname):
        # must set tcp url in docker startup
        docker_client = self.get_docker_client(hostname)
        return [docker_client.ps_info(c) for c in docker_client.containers.list()]

    def agent_docker_ps(self):
        host_container_ps = {}
        for agent in self.mesos_agents():
            hostname = agent.hostname
            host_container_ps[hostname] = self._agent_docker_ps(hostname)
        return host_container_ps

    def container_blkio_stats(self, hostname, cid):
        docker_client = self.get_docker_client(hostname)
        container = docker_client.containers.get(cid)
        r = container.stats(stream=False)["blkio_stats"]["io_service_bytes_recursive"]
        return max([f["value"] for f in r if f["op"] == "Read"])

    def is_blkio_trigger(self, value):
        return value / 1024.0 / 1024 / 1024 > 15

    def stop_container(self, hostname, cid):
        docker_client = self.get_docker_client(hostname)
        container = docker_client.containers.get(cid)
        return container.stop()

    def monitor(self):
        agent_containers = self.agent_docker_ps()
        for hostname, cinfos in agent_containers.iteritems():
            for ci in cinfos:
                blkio_value = self.container_blkio_stats(hostname, ci["id"])
                if self.is_blkio_trigger(blkio_value):
                    self.stop_container(hostname, ci["id"])