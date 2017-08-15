#!/usr/bin/env python
# coding=utf-8

import sys
sys.path.append("..")

import marathon
import config
import mesos_api
import logging
import utils.lib_util
from marathon.models import MarathonConstraint
from marathon.models.container import MarathonContainer, MarathonContainerVolume

logger = logging.getLogger(__name__)


class MarathonHelper(object):

    def __init__(self, marathon_uri=config.MARATHON_URI, **kwargs):
        self.client = marathon.MarathonClient(marathon_uri, **kwargs)
        self.proxy = utils.lib_util.APIProxy(self.client)

    def __getattr__(self, name):
        return getattr(self.proxy, name)

    def update_app_docker_image(self, id, image):
        app = self.client.get_app(id)
        if app is None:
            return None
        app.container.docker.image = image
        return self.client.update_app(id, app)

    def _get_app(self, id):
        app = self.client.get_app(id)
        if app is None:
            return None
        return app

    def change_contraint(self, id, constraints):
        app = self.client.get_app(id)
        if app is None:
            return None
        assert len(constraints) > 0
        assert isinstance(constraints[0], tuple) or isinstance(constraints[0], list)
        app.constraints = [MarathonConstraint(*constraint) for constraint in constraints]
        return self.client.update_app(id, app)

    def update_volumes(self, id, volumes):
        app = self._get_app(id)
        if app and volumes and len(volumes):
            app_volumes = [MarathonContainerVolume(**v) for v in volumes]
            app.container.volumes = app_volumes
            return self.client.update_app(id, app)

    def update_env(self, id, env):
        assert isinstance(env, dict)
        logger.info("update app : %s env" % id)
        app = self._get_app(id)
        if app and app.env != env:
            app.env = env
            return self.client.update_app(id, app)



if __name__ == '__main__':
    helper = MarathonHelper(username=config.MARATHON_USER, password=config.MARATHON_PASSWD)
    print helper.list_endpoints()
