# !/usr/bin/env python
# coding=utf-8

import logging
import urlparse
import requests
import collections

logger = logging.getLogger(name="Burrow")

KafkaConsumerLag = collections.namedtuple("kafkaConsumerLag", ["group", "status", "complete", "partition_count", "totallag", "partitions", "cluster", "maxlag"])

class UrlException(Exception):
    def __init__(self, url, msg):
        self.url = url
        self.msg = msg

    def __repr__(self):
        return "url: {0} exception: {1}".format(self.url, self.msg)

    def __str__(self):
        return "url: {0} exception: {1}".format(self.url, self.msg)


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
            logger.exception(e)
            raise
        return r
    return _check
            

class BurrowApi(object):

    def __init__(self, addr, **kwargs):
        self.addr = addr
        self.kwargs = kwargs

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
            logger.exception(e)
            return None
        return resp

    def health(self):
        url = urlparse.urljoin(self.addr, "/burrow/admin")
        r = requests.get(url)
        return r.content == "GOOD"

    def cluster(self, name=None):
        if name is None:
            url = urlparse.urljoin(self.addr, "/v2/kafka")
        else:
            url = urlparse.urljoin(self.addr, "/v2/kafka/%s" % name)
        rjson = self._do_request(url, "GET").json()
        return rjson["cluster"]

    def consumers(self, cluster):
        url = urlparse.urljoin(self.addr, "/v2/kafka/{0}/consumer".format(cluster))
        rjson = self._do_request(url, "GET").json()
        return rjson["consumers"]

    def delete_consumer(self, cluster, grp):
        url = urlparse.urljoin(self.addr, "/v2/kafka/{0}/consumer/{1}".format(cluster, grp))
        rjson = self._do_request(url, "DELETE").json()
        return rjson["message"]

    def consumer_topics(self, cluster, grp):
        url = urlparse.urljoin(self.addr, "/v2/kafka/{0}/consumer/{1}/topic".format(cluster, grp))
        rjson = self._do_request(url, "GET").json()
        return rjson["topics"]

    def consumer_topic_offset(self, cluster, grp, topic):
        url = urlparse.urljoin(self.addr, "/v2/kafka/{0}/consumer/{1}/topic/{2}".format(cluster, grp, topic))
        rjson = self._do_request(url, "GET").json()
        return rjson["offsets"]

    def _consumer_topic_status(self, json_status, topic=None):
        if topic is None:
            return json_status
        topic_pats = [ tp for tp in json_status["partitions"] if tp["topic"] == topic ]
        status = all(map(lambda x: x["status"] == 'OK', topic_pats))
        json_status["status"] = "ERR" if not status else "OK"
        json_status["complete"] = status
        json_status["totallag"] = sum([ tp["end"]["lag"] for tp in topic_pats ])
        json_status["partition_count"] = len(topic_pats)
        json_status["partitions"] = topic_pats
        json_status["maxlag"] = sorted(topic_pats, key=lambda x: x["end"]["lag"], reverse=True)[0]
        return json_status

    def consumer_status(self, cluster, grp, topic=None):
        url = urlparse.urljoin(self.addr, "/v2/kafka/{0}/consumer/{1}/status".format(cluster, grp))
        rjson = self._do_request(url, "GET").json()
        return self._consumer_topic_status(rjson["status"], topic)

    def consumer_lag(self, cluster, grp, topic=None):
        url = urlparse.urljoin(self.addr, "/v2/kafka/{0}/consumer/{1}/lag".format(cluster, grp))
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
        url = urlparse.urljoin(self.addr, "/v2/kafka/{0}/topic/{1}".format(cluster, topic))
        rjson = self._do_request(url, "GET").json()
        return rjson["offsets"]
 
    def topics(self, cluster):
        url = urlparse.urljoin(self.addr, "/v2/kafka/{cluster}/topic".format(cluster=cluster))
        r = self._do_request(url, "GET")
        rjson = r.json()
        return rjson["topics"]

if __name__ == '__main__':
    import json
    client = BurrowApi("http://127.0.0.1:9000")
    print json.dumps(client.consumer_lag("yg_kafka", "hangout", "new_app_error"), indent=2)
