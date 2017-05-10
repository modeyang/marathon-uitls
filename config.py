# !/usr/bin/env python
# coding=utf-8

import os
import logging

DEBUG = False
log_format="%(asctime)s %(filename)s [funcname:%(funcName)s] [line:%(lineno)d] %(levelname)s %(message)s"
logging.basicConfig(level=logging.INFO if not DEBUG else logging.DEBUG, format=log_format)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

MESOS_URI=os.getenv("MESOS_URI", "http://127.0.0.1:5050")

MARATHON_URI=os.getenv("MARATHON_URI", "http://127.0.0.1:8080")

MARATHON_USER=os.getenv("MARATHON_USER", "root")
MARATHON_PASSWD = os.getenv("MARATHON_PASSWD", "root")

MAX_FILE_TIMEOUT = 3600 * 60

try:
    from local_settings import *
except Exception, e:
    pass
