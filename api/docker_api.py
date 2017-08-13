# !/usr/bin/env python
# coding=utf-8

import docker
from utils import APIProxy


class DockerHelper(object):

    def __init__(self, docker_url, *args, **kwargs):
        self.client = docker.DockerClient(base_url=docker_url)
        self.proxy = APIProxy(self.client)

    def __getattr__(self, name):
        return getattr(self.proxy, name)

    def ps_info(self, container):
        attrs = ["id", "short_id", "name", "image", "labels"]
        return dict(zip(attrs, [container.getattr(a) for a in attrs]))

if __name__ == "__main__":

    helper = DockerHelper("tcp://10.100.1.144:2375")
    print helper.containers()