# !/usr/bin/env python
# coding=utf-8

import os
import json
from config import MARATHON_USER, MARATHON_PASSWD
from api.marathon_api import MarathonHelper
from api.docker_api import DockerHelper
from api.mesos_api import MesosHelper


# c = MarathonHelper(username=MARATHON_USER, password=MARATHON_PASSWD)
# print c.list_apps()
# 
# app = "/logstash/ott-nginx"
# 
# app_tasks = c.get_app(app, embed_tasks=True, embed_task_stats=True, embed_counts=True)
# print app_tasks
# 
# app = "cadvisor"
# app_tasks = c.list_tasks(app)
# 
# print app_tasks[0].to_json()

class MesosDocker(object):
    AGENT_CACHE = {}

    def __init__(self, *args, **kwargs):
        self.mesos_client = MesosHelper() 

    def mesos_agents(self):
        return self.mesos_client.agents()

    def get_docker_client(self, hostname):
        docker_client = None
        if hostname in self.AGENT_CACHE:
            docker_client = self.AGENT_CACHE[hostname]
        else:
            url = "tcp://" + hostname + "2375"
            docker_client = DockerHelper(url)
            self.AGENT_CACHE[hostname] = docker_client
        return docker_client

    def _agent_docker_ps(self, hostname):
        # must set tcp url in docker startup
        agent_container_ps = []
        docker_client = self.get_docker_client(hostname)
        return [ docker_client.ps_info(c) for c in docker_client.containers.list() ]

    def agent_docker_ps(self):
        host_container_ps = {}
        for hostname in self.mesos_agents():
            host_container_ps[hostname] = self._agent_docker_ps(hostname)
        return host_container_ps

    def container_blkio_stats(self, hostname, id):
        docker_client = self.get_docker_client(hostname)
        container = docker_client.containers.get(id)
        r = container.stats(stream=False)["blkio_stats"]["io_service_bytes_recursive"]
        return max([f["value"] for f in r if f["op"] == "Read"])

    def is_blkio_trigger(self, value):
        return value / 1024.0 / 1024 / 1024 > 1.5

    def stop_container(self, hostname, id):
        docker_client = self.get_docker_client(hostname)
        container = docker_client.containers.get(id)
        yield container.stop()

    def run(self):
        agent_containers = self.agent_container_ps()
        for hostname, cinfo in agent_containers.iteritems():
            if is_blkio_trigger(hostname, cinfo["id"]):
                self.stop_container(cinfo["id"])
        