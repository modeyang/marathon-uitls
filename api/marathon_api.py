#!/usr/bin/env python
# coding=utf-8

import sys
sys.path.append("..")

import os
import marathon
import config
import mesos_api
import logging
import json
import utils.lib_util
from marathon.models import MarathonConstraint
from marathon.models.container import MarathonContainer, MarathonContainerVolume

logger = logging.getLogger(__name__)

APP_STATUS_FILE = os.path.join(config.BASE_PATH, "app_status.json")

def save_metrics(file_path):
    def _save(func):
        def __decorator(*args, **kwargs):
            rjson = func(*args, **kwargs)
            if rjson is None:
                return None
            with open(file_path, "w") as f:
                f.write(rjson + "\n")
            return rjson
        return __decorator
    return _save


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

    def scale_group_apps(self, include_apps=[], instances=0):
        assert isinstance(include_apps, (list, tuple))
        grps = self.client.list_groups()
        for grp in grps:
            for app in grp.apps:
                if len(include_apps) > 0 and not all([ f in app.id for f in include_apps]):
                    continue
                # print app.id
                self.client.scale_app(app.id, instances)

    @save_metrics(APP_STATUS_FILE)
    def pause_groups(self, groups=[], **kwargs):
        filter_apps = ["nginx", "packet", "sla", "playhistory_app", "audit"]
        # filter_apps = ["nginx", "packet", "sla"]

        def is_filter_app(appid, filters):
            for f in filters:
                if f in appid:
                    return True
            return False

        app_status = {}
        grps = self.client.list_groups()
        filter_grps = [ grp for grp in grps if grp.id in groups ] 
        for grp in filter_grps:
            for app in grp.apps:
                if is_filter_app(app.id, filter_apps):
                    app_status[app.id] = app.instances
                    self.client.scale_app(app.id, 0)
        return json.dumps(app_status)

    def restore_apps(self, file_path=APP_STATUS_FILE, instances=1):
        app_status = json.loads(open(file_path, "r").read())
        for app_id, instances in app_status.iteritems():
            try:
                self.client.scale_app(app_id, instances)
            except Exception, e:
                print e


if __name__ == '__main__':
    helper = MarathonHelper(username=config.MARATHON_USER, password=config.MARATHON_PASSWD)
    # helper.pause_groups(["/hangout", "/logstash"])
    helper.restore_apps()
