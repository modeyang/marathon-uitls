# !/usr/bin/env python
# coding=utf-8

import click

import config
from services.logstash_monitor import LogstashMonitor
from services.kafka_consumer_monitor import KafkaConsumerMonitor
from api.autoscale import DeciderManager

@click.command()
@click.option("--app", default="monitor", help="logstash monitor or autoscale")
def main(app):
    if app == "monitor":
        # m = LogstashMonitor()
        m = KafkaConsumerMonitor()
        m.check()
    else:
        decide = DeciderManager()
        decide.make_descide()
        

if __name__ == "__main__":
    main()