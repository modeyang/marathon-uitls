#!/usr/bin/env python
# coding=utf-8

import marathon
import config
import mesos_api
import logging
import lib_util
from marathon.models import MarathonApp

logger = logging.getLogger(__name__)


class MarathonHelper(object):

    def __init__(self, marathon_uri=config.MARATHON_URI, **kwargs):
        self.client = marathon.MarathonClient(marathon_uri, **kwargs)
        self.proxy = lib_util.APIProxy(self.client)

    def __getattr__(self, name):
        return getattr(self.proxy, name)




if __name__ == '__main__':
    helper = MarathonHelper(username=config.MARATHON_USER, password=config.MARATHON_PASSWD)
    print helper.list_apps(cmd="web")
    print helper.list_endpoints()
    print helper.get_app("zk-web")
    print helper.list_event_subscriptions()