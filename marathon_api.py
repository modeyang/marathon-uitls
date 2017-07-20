#!/usr/bin/env python

import json
import requests

url = "http://10.100.1.211:8080/v2/tasks"

headers = {
	"Cookie": "OUTFOX_SEARCH_USER_ID_NCOO=843063764.2383163",
	"Authorization": "Basic cm9vdDptZ3R2"
}

# r = requests.get(url, headers=headers)

# delete_tasks = []
# for task in r.json()["tasks"]:
# 	if "consule" in task["id"]:
# 		if "1f4b-11e7-a60a-02427fdf5612" in task["id"]:
# 			continue
# 		delete_tasks.append(task["id"]) 


delete_tasks = [u'consule.7964a283-2c0b-11e7-8b67-3ee821cd9c56', u'consule.92392425-2c0b-11e7-8b67-3ee821cd9c56', u'consule.86000574-2c0b-11e7-8b67-3ee821cd9c56']
# delete_url = "http://10.100.1.211:8080/v2/tasks/delete?force=true"
mesos_del_url = "http://10.100.1.144:5050/api/v1/executor"
headers["Content-Type"] = "application/json"
data = {
	"ids": delete_tasks
}

for task in delete_tasks:
	data = {
	  "type" : "KILL",
	  "kill" : {
	    "task_id" : {"value" : task}
	  }
	}
	print data
	r = requests.post(mesos_del_url, data=json.dumps(data), headers=headers)
	print r.content
# print json.dumps(data)
# r = requests.post(delete_url, data=json.dumps(data), headers=headers)

# url = "http://zk-web.intra.hunantv.com/node?path=/marathon_mgtv_cloud/state"
# r = requests.get(url)
# print r.content