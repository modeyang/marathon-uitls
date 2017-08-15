# !/usr/bin/env python
# coding=utf-8

import json
import docker
from utils import APIProxy


def Env2Dict(env_list):
    env_dict = {}
    for env in env_list:
        key, value = env.split("=", 1)
        env_dict[key] = value
    return env_dict


class DockerHelper(object):

    def __init__(self, docker_url, *args, **kwargs):
        self.client = docker.DockerClient(base_url=docker_url, **kwargs)
        self.proxy = APIProxy(self.client)

    def __getattr__(self, name):
        return getattr(self.proxy, name)

    @classmethod
    def ps_info(cls, container):
        # attrs = ["id", "short_id", "name", "labels"]
        attrs = ["id", "short_id", "name"]
        _ps_dict = dict(zip(attrs, [getattr(container, a) for a in attrs]))
        _ps_dict.update(Env2Dict(container.attrs["Config"]["Env"]))
        return _ps_dict

