# !/usr/bin/env python
# coding=utf-8

import os
import time
import logging
import requests
import config
import urlparse
import json
import collections

logger = logging.getLogger("mesos")

# delete framework
# curl -XPOST http://localhost:5050/master/teardown -d 'frameworkId=af404bcf-df0d-4f29-b542-adc635031512-0002'
def dump_state(file_path):
    def _dump_state(func):
        def __decorator(*args, **kwargs):
            if os.path.exists(file_path):
                pt = os.stat(file_path)
                now = int(time.time())
                if pt.st_size > 0 and now - pt.st_mtime < config.MAX_FILE_TIMEOUT:
                    logger.info("load state from file -> %s" % file_path)
                    return json.loads(open(file_path, "r").read())

            rjson = func(*args, **kwargs)
            if rjson is None:
                return None

            with open(file_path, "w") as f:
                f.write(json.dumps(rjson, indent=2))
            return rjson
        return __decorator
    return _dump_state


class MesosHelper(object):
    def __init__(self, mesos_addr=config.MESOS_URI, **kwargs):
        self.kwargs = kwargs
        self.mesos_addr = mesos_addr
        self.framework_id = None

    def _do_request(self, method, path, params=None, data=None, **kwargs):
        headers = {'Content-Type': 'application/json'}
        url = urlparse.urljoin(self.mesos_addr, path)
        try:
            response = requests.request(method, url, params=params, data=data, headers=headers, **kwargs)
        except requests.exceptions.RequestException, e:
            logger.error('Error while calling %s: %s', url, e.message)

        if response is None:
            raise Exception("mesos server no response , please check it, %s" % response.content)

        try:
            return response.json()
        except Exception, e:
            logger.error(response.content)
            logger.error(e)
            return None

    def delete_framework(self, framework_id):
        data = {
            "frameworkId": framework_id
        }
        return self._do_request("POST", "/master/teardown", data=data)

    def metrics(self):
        return self._do_request("GET", "/metrics/snapshot")

    @dump_state("state.json")
    def state(self):
        return self._do_request("GET", "/metrics/state")

    def frameworks(self, **filter_kwargs):
        rjson = self.state()
        _frameworks = rjson["frameworks"]
        active = filter_kwargs.pop("active", None)
        if active is not None:
            _frameworks = filter(lambda x: x["active"] == active, _frameworks)

        framework_id = filter_kwargs.pop("framework_id", None)
        if framework_id is not None:
            _frameworks = filter(lambda x: x["id"] == framework_id, _frameworks)
        return _frameworks

    def framework_ids(self, **filter_kwargs):
        _frameworks = self.frameworks(**filter_kwargs)
        return [x["id"] for x in _frameworks ]

    def state_from_segment(self, segment_name):
        rjson = self.state()
        if segment_name not in rjson:
            return None
        return rjson[segment_name]

    def get_framework_app_tasks(self, framework_id="2b2a4298-7855-46f5-97bb-0e9879325e5c-0000"):
        self.framework_id = framework_id
        framework_info = self.frameworks(framework_id=framework_id)[0]
        tasks = framework_info["tasks"]
        app_map = collections.defaultdict(list)
        for t in tasks:
            app_map[t["name"]].append(t)
        return app_map


if __name__ == '__main__':
    util = MesosHelper()
    # print util.metrics()
    util.delete_framework("2b2a4298-7855-46f5-97bb-0e9879325e5c-0000")
    # app_map = util.get_framework_app_tasks()
    # print util.state_from_segment("unregistered_frameworks")
    # print util.metrics()
