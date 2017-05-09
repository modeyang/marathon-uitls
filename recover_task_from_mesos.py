#!/usr/bin/env python
# coding=utf-8

import click
import config
import json
import logging
import marathon_util
import mesos_api
from marathon.models import *
from marathon.models.container import MarathonContainer, MarathonContainerVolume

logger = logging.getLogger(__name__)


class RecoverTaskFromMesos(object):
  '''
  marathon recover app tasks from mesos 
  '''

  def __init__(self):
    self.api = mesos_api.MesosHelper()
    self.marathon_api = marathon_util.MarathonHelper(
        username=config.MARATHON_USER, 
        password=config.MARATHON_PASSWD
      )
    self.running_tasks = self.api.get_framework_tasks()
    self.marathon_apps = self.get_all_apps()

  def get_all_apps(self):
    apps = self.marathon_api.list_apps()
    return [ app.id for app in apps ]

  def _exists(self, app_name):
    return app_name in self.marathon_apps

  def get_apps_by_tag(self, tag):
      return filter(lambda x: tag in x[0], self.running_tasks.items())

  def recover_marathon_lb(self):
    apps =  self.get_apps_by_tag("marathon-lb")
    constraint = MarathonConstraint("hostname", "LIKE", "10.100.1.211")
    return self._recover_app_from_mesos(apps[0], constraint)

  def recover_docker_registry(self):
    apps = self.get_apps_by_tag("docker-registry")
    constraint = MarathonConstraint("hostname", "LIKE", "10.100.6.4")

  def recover_cadvisor(self):
    apps = self.get_apps_by_tag("cadvisor")
    constraint = MarathonConstraint("hostname", "UNIQUE")
    return self._recover_app_from_mesos(apps[0], constraint)

  def recover_logstash_tasks(self):
    apps = self.get_apps_by_tag("logstash")
    constraint = MarathonConstraint("name", "UNLIKE", "web")
    return [self._recover_app_from_mesos(app, constraint) for app in apps]

  def recover_pypy_tasks(self):
    apps = self.get_apps_by_tag("pypy_")
    constraint = MarathonConstraint("name", "LIKE", "worker")
    return [self._recover_app_from_mesos(app, constraint) for app in apps]

  def _recover_app_from_mesos(self, mesos_app, constraint=None, **kwargs):
      name, tasks = mesos_app
      names = name.split(".")
      if len(names) > 1:
          reverse_names = reversed(names)
          name = "/".join(reverse_names)
      name = "/" + name.strip()

      if self._exists(name):
        logger.info("app %s has exists in marathon" % name)
        return

      instances = len(tasks)
      task = tasks[0]
      constraint = MarathonConstraint("name", "LIKE", "worker") if constraint is None else constraint
      
      volumes = task["container"]["volumes"]
      app_volumes = []
      if len(volumes) > 0:
        app_volumes = [MarathonContainerVolume(**v) for v in volumes]

      container = MarathonContainer(
                    docker=task["container"]["docker"], 
                    type=task["container"]["type"], 
                    volumes=app_volumes
                  )

      labels = {}
      if "labels" in task:
          for label in task["labels"]:
              labels[label["key"]] = label["value"]

      app = MarathonApp(instances=instances, 
          id=name, 
          mem=task["resources"]["mem"], 
          cpus=task["resources"]["cpus"],
          container=container,
          constraints=[constraint])
      if len(labels) > 0:
          app.labels = labels
      return self.marathon_api.create_app(name, app)

  def do(self):
    # logger.info(self.get_all_apps())
    # self.recover_cadvisor()
    self.recover_marathon_lb()


if __name__ == '__main__':
  RecoverTaskFromMesos().do()