# !/usr/bin/env python
# coding=utf-8

import click

import config
from services.logstash_monitor import LogstashMonitor
from services.kafka_consumer_monitor import KafkaConsumerMonitor
from api.autoscale import DeciderManager
from task_util import task_manager, init_signals 

@click.command()
@click.option("--app", default="monitor", help="logstash monitor or autoscale")
def main(app):
    if app == "monitor":
        # m = LogstashMonitor()
        m = KafkaConsumerMonitor()
        task_manager.add_interval_task(m.check, seconds=config.MONITOR_INTERVAL)
        task_manager.run()
    else:
        decide = DeciderManager()
        decide.make_descide()
        

if __name__ == "__main__":
    init_signals()
    main()