# !/usr/bin/env python
# coding=utf-8

import logging
import urlparse
import requests
import collections


KafkaConsumerLag = collections.namedtuple("kafkaConsumerLag", ["group", "status", "complete", "partition_count", "totallag", "partitions", "cluster", "maxlag"])

class UrlException(Exception):
    def __init__(self, url, msg):
        self.url = url
        self.msg = msg

    def __repr__(self):
        return "url: {0} exception: {1}".format(self.url, self.msg)

    def __str__(self):
        return "url: {0} exception: {1}".format(self.url, self.msg)


class BurrowVersionNotFound(Exception):
    pass


def check_rjson_error(f):
    def _check(*args, **kwargs):
        r = f(*args, **kwargs)
        try:
            rjson = r.json()
            error = rjson["error"]
            url = rjson["request"]["url"]
            if error:
                raise UrlException(url, rjson["message"])
        except UrlException as e:
            logging.exception(e)
            raise
        return r
    return _check
            

class BurrowApi(object):

    def __init__(self, addr, version="v2", **kwargs):
        self.addr = addr
        self.kwargs = kwargs
        self.version = version

    @check_rjson_error
    def _do_request(self, url, method, data=None, params=None):
        resp = None
        try:
            if method == "GET":
                resp = requests.get(url, params=params)
            elif method == "POST":
                resp = requests.post(url, params=params, data=data)
            elif method == "DELETE":
                resp = requests.delete(url, params=params)
            else:
                pass
        except Exception as e:
            logging.exception(e)
            return None
        return resp

    def health(self):
        url = urlparse.urljoin(self.addr, "/burrow/admin")
        r = requests.get(url)
        return r.content == "GOOD"

    def cluster(self, name=None):
        if self.version == "v2":
            return self._v2_cluster(name)
        elif self.version == "v3":
            return self._v3_cluster(name)
        raise BurrowVersionNotFound() 

    def _v2_cluster(self, name=None):
        if name is None:
            url = urlparse.urljoin(self.addr, "/{0}/kafka".format(self.version))
        else:
            url = urlparse.urljoin(self.addr, "/{0}/kafka/{1}".format(self.version, name))
        rjson = self._do_request(url, "GET").json()
        return rjson["clusters"] if name is None else rjson["cluster"]

    def _v3_cluster(self, name=None):
        if name is None:
            url = urlparse.urljoin(self.addr, "/{0}/kafka".format(self.version))
        else:
            url = urlparse.urljoin(self.addr, "/{0}/kafka/{1}".format(self.version, name))
        rjson = self._do_request(url, "GET").json()
        return rjson["clusters"] if name is None else rjson["module"]

    def consumers(self, cluster):
        url = urlparse.urljoin(self.addr, "/{0}/kafka/{1}/consumer".format(self.version, cluster))
        rjson = self._do_request(url, "GET").json()
        return rjson["consumers"]

    def delete_consumer(self, cluster, grp):
        url = urlparse.urljoin(self.addr, "/{0}/kafka/{1}/consumer/{2}".format(self.version, cluster, grp))
        rjson = self._do_request(url, "DELETE").json()
        return rjson["message"]

    def consumer_topics(self, cluster, grp):
        if self.version == "v3":
            logging.error("burrow v3 api not support")
            return None

        url = urlparse.urljoin(self.addr, "/{0}/kafka/{1}/consumer/{2}/topic".format(self.version, cluster, grp))
        rjson = self._do_request(url, "GET").json()
        return rjson["topics"]

    def consumer_topic_offset(self, cluster, grp, topic):
        if self.version == "v3":
            logging.error("burrow v3 api not support")
            return None
            
        url = urlparse.urljoin(self.addr, "/{0}/kafka/{1}/consumer/{2}/topic/{3}".format(self.verison, cluster, grp, topic))
        rjson = self._do_request(url, "GET").json()
        return rjson["offsets"]

    def _consumer_topic_status(self, json_status, topic=None):
        if topic is None:
            return json_status
        topic_pats = [ tp for tp in json_status["partitions"] if tp["topic"] == topic ]
        if len(topic_pats) == 0:
            return None
        status = all(map(lambda x: x["status"] == 'OK', topic_pats))
        json_status["status"] = "ERR" if not status else "OK"
        json_status["complete"] = status
        json_status["totallag"] = sum([ tp["end"]["lag"] for tp in topic_pats ])
        json_status["partition_count"] = len(topic_pats)
        json_status["partitions"] = topic_pats
        json_status["maxlag"] = sorted(topic_pats, key=lambda x: x["end"]["lag"], reverse=True)[0]
        return json_status

    def consumer_status(self, cluster, grp, topic=None):
        url = urlparse.urljoin(self.addr, "/{0}/kafka/{1}/consumer/{2}/status".format(self.version, cluster, grp))
        rjson = self._do_request(url, "GET").json()
        return self._consumer_topic_status(rjson["status"], topic)

    def consumer_lag(self, cluster, grp, topic=None):
        url = urlparse.urljoin(self.addr, "/{0}/kafka/{1}/consumer/{2}/lag".format(self.version, cluster, grp))
        rjson = self._do_request(url, "GET").json()
        return self._consumer_topic_status(rjson["status"], topic)

    def consumer_lag_obj(self, cluster, grp, topic=None):
        lag_status = self.consumer_lag(cluster, grp, topic)
        return KafkaConsumerLag(**lag_status) 

    def consumer_lag_json(self, cluster, grp, topic=None):
        lag_status = self.consumer_lag(cluster, grp, topic)
        kafka_lag = KafkaConsumerLag(**lag_status) 
        partition_status = collections.Counter([ p["status"] for p in kafka_lag.partitions ])
        consumer_status = {
            "partition_status" : partition_status,
            "status": kafka_lag.status,
            "complete": kafka_lag.complete,
            "partition_count": kafka_lag.partition_count,
            "totallag": kafka_lag.totallag,
        }
        return consumer_status

    def topic_offset(self, cluster, topic):
        url = urlparse.urljoin(self.addr, "/{0}/kafka/{1}/topic/{2}".format(self.version, cluster, topic))
        rjson = self._do_request(url, "GET").json()
        return rjson["offsets"]
 
    def topics(self, cluster):
        url = urlparse.urljoin(self.addr, "/{0}/kafka/{1}/topic".format(self.version, cluster))
        r = self._do_request(url, "GET")
        rjson = r.json()
        return rjson["topics"]

if __name__ == '__main__':
    import json
    client = BurrowApi("http://127.0.0.1:8005", "v3")
    print client.consumer_topic_offset("yg_kafka", "app_cruiser", "new_app_error")
