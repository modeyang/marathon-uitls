# !/usr/bin/env python
# coding=utf-8

import os
from config import MARATHON_USER, MARATHON_PASSWD
from api.marathon_api import MarathonHelper


c = MarathonHelper(username=MARATHON_USER, password=MARATHON_PASSWD)
print c.list_apps()

app = "/logstash/ott-nginx"

app_tasks = c.get_app(app, embed_tasks=True, embed_task_stats=True, embed_counts=True)

print app_tasks

app = "cadvisor"
app_tasks = c.list_tasks(app)

print app_tasks[0].to_json()