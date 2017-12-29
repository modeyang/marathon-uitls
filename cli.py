#!/usr/bin/env python
# -*- coding:utf-8 -*-
# vim: set number tw=0 shiftwidth=4 tabstop=4 expandtab:

import fire
import config
from api.marathon_api import MarathonHelper


class MarathonCli(object):
    def __init__(self):
        self.api = MarathonHelper(username=config.MARATHON_USER, password=config.MARATHON_PASSWD)

    def group(self, action, groups="/logstash,/hangout"):
        assert action in ["pause", "start"]
        if action == "pause":
            m_grps = groups.split(",")
            self.api.pause_groups(m_grps)
        elif action == "start":
            self.api.restore_apps()

    def app(self, apps, action):
        assert action in ["pause", "start"]


if __name__ == '__main__':
    fire.Fire(MarathonCli)