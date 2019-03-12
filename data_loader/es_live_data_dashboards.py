import requests
from elasticsearch import Elasticsearch
import datetime
import re

r = requests.get("http://localhost:9200/.kibana/_search?pretty")

r_json = r.json()

print(r_json)
es = Elasticsearch()
es_payload = {}

hits_lst = r_json['hits']['hits']
dashboard_id = ''
for hit in hits_lst:
    if hit['_source']['type'] == 'dashboard':
        es_payload['timestamp'] = datetime.datetime.today().strftime('%Y-%m-%d')
        dashboard_json = hit['_source']['dashboard']
        m = re.search('"id":"(.+?)","', dashboard_json['panelsJSON'])
        if m:
            dashboard_id = m.group(1)
        print(dashboard_id)
        timeTo = str(dashboard_json['timeTo'])
        es_payload['filename'] = "http://localhost:5601/app/kibana#/visualize/edit/" + str(dashboard_id) + "?_g=(refreshInterval:(display:'" + \
                                 hit['_source']['dashboard']['refreshInterval']['display']  + "',pause:" + str(dashboard_json['refreshInterval']['pause']) + ",section:" + str(dashboard_json['refreshInterval']['section']) + \
                                 ",value:" + str(dashboard_json['refreshInterval']['value']) + \
                                 "),time:(from:" + str(dashboard_json['timeFrom']) + \
                                 ",mode:quick,to:" + str(timeTo) + "))&_a=(vis:(aggs:!((schema:metric,type:count),(params:(field:geo_location),schema:segment,type:geohash_grid))))"

        es_payload['topics'] = 'Live Data'
        es_payload['uniqueObjects'] = 'Dashboards'
        es_payload['actualtext'] = hit['_source']['dashboard']['description']
        print(es_payload)

        res = es.index(index='live-data-' + datetime.datetime.today().strftime('%Y-%m-%d'), doc_type='visualization', id=1,
                   body=es_payload)
        print(res['result'])
