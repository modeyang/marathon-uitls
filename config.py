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

DOCKER_URL=os.getenv("DOCKER_URL", "unix:///var/run/docker.sock")
DOCKER_TIMEOUT = int(os.getenv("DOCKER_TIMEOUT", 30))
MODE=os.getenv("MODE", "cluster")

MAX_FILE_TIMEOUT = 3600 * 60

# kafka configs
BURROW_URI = os.getenv("BURROW_URI", "http://127.0.0.1:9000")
KAFKA_CLUSTER = os.getenv("KAFKA_CLUSTER", "yg_kafka")
KAFKA_CONFIG_FILE = os.getenv("KAFKA_CONFIG_FILE", "kafka_lag.yml")
KAFKA_ZK = os.getenv("KAFKA_ZK", "127.0.0.1:2181")
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "127.0.0.1:9092")
DEFAULT_KAFKA_TIMEOUT = int(os.getenv("KAFKA_TIMEOUT", 10))
DEFAULT_ZK_TIMEOUT = int(os.getenv("ZK_TIMEOUT", 5)) 

try:
    from local_settings import *
except Exception, e:
    pass
