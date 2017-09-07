# !/usr/bin/env python
# coding=utf-8

from influxdb import InfluxDBClient
from datetime import datetime
import logging
import config

logger = logging.getLogger("__file__") 


class InfluxHelper(object):

    def __init__(self, database, **kwargs):
        dsn = kwargs.pop("dsn", config.INFLUXDB_DSN)
        uri = dsn + database
        self.client = InfluxDBClient.from_DSN(uri)
        self.client.create_database(database)
        self.client.switch_database(database)
    
    def __check_json_body(self, json_body):
        if type(json_body) != type([]):
            return False, "body is not list"
        if len(json_body) == 0:
            return False, "json body no data"
        body_item = json_body[0]

        fields = ["measurement", "fields", "time"]
        if len(set(body_item.keys()) & set(fields)) == 0:
            return False, "body item must have fields with %s " % fields
        return True, "success"

    def write_data(self, json_body):
        status, msg = self.__check_json_body(json_body)
        if not status:
            logging.error(msg)
        return self.client.write_points(json_body)

    def kv(self, txt, field_split=",", kv_split="="):
        kv_dict = {}
        for value in txt.split(field_split, 1):
            key, data = value.split(kv_split, 1)
            kv_dict[key] = data
        return kv_dict

    def bulk_metrics(self, metrics, timestamp):
        """                     
        metric format: metric_name/k1=v1,k2=v2                                                                                                                                                                                       
        tranform to influxdb format: 
            {                   
                "measurement": metric_name,
                "tags": {       
                    k1 : v1,    
                    k2 : v2     
                },              
                "time": <timestamp format>,
                "fields": {     
                    "value": <value>
                }
            }                   
        """
        sink_metrics = []       
        offset = 8 * 3600       
        time_str = datetime.fromtimestamp(timestamp - offset).strftime("%Y-%m-%dT%H:%M:%SZ")
        for key, value in metrics.iteritems():
            measurement, tags = key.split("/", 1)
            kv_tags = self.kv(tags, ",", "=")
            if kv_tags is None:              
                continue
            influx_metric = {                
                "measurement": measurement,      
                "tags": kv_tags,                 
                "time": time_str,                
                "fields": {                      
                    "value": value                   
                }       
            }           
            sink_metrics.append(influx_metric)  
        self.write_data(sink_metrics)

    def query(self, sql, filters):
        rs = self.client.query(sql)
        return rs.get_points(filters)   