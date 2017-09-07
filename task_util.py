#!/usr/bin/python
# -*- coding:utf-8 -*-
# vim: set number tw=0 shiftwidth=4 tabstop=4 expandtab:
import os
import signal
import logging
import threading
from apscheduler.schedulers.blocking import BlockingScheduler
    
logger = logging.getLogger(__file__)


class TaskManager(threading.Thread):

    def __init__(self, **kwargs):
        super(TaskManager, self).__init__()
        self.scheduler = BlockingScheduler({'max_instances': 1})

    def add_interval_task(self, task, **kwargs):
        self.scheduler.add_job(task, "interval", **kwargs)

    def add_cron_task(self, task, **kwargs):
        self.scheduler.add_job(task, "cron", **kwargs)

    def run(self):
        try:
            self.scheduler.start()
        except Exception as e:
            logger.exception(e)

    def stop(self):
        self.scheduler.shutdown()

task_manager = TaskManager()


def signal_handler(sig, frame):
    task_manager.stop()
    pid = os.getpid()
    os.kill(pid, signal.SIGQUIT)


def init_signals():
    signals = [signal.SIGILL, signal.SIGSEGV, signal.SIGTERM, signal.SIGINT]
    map(lambda x: signal.signal(x, signal_handler), signals)