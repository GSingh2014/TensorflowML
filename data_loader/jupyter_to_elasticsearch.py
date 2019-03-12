from elasticsearch import Elasticsearch
import os
import datetime
import shutil

es = Elasticsearch()
es_payload = {}
directory = 'C:\\Users\\singhgo\\Documents\\work\\dev\\TensorML\\ds\\jupyter'
for filename in os.listdir(directory):
    if filename.endswith(".ipynb"):
        print(filename)
        shutil.copy(directory + '\\' + filename,
                    'C:\\Users\\singhgo\\Documents\\work\\dev\\TensorML\\ui\\DataLake_Search\\static')
        es_payload['timestamp'] = datetime.datetime.today().strftime('%Y-%m-%d')
        es_payload['filename'] = filename
        es_payload['topics'] = 'Data Science'
        es_payload['uniqueObjects'] = 'Jupyter Notebook'
        es_payload['actualtext'] = 'Graph analytics of Bus traffic using Jupyter python notebook'
        print(es_payload)

res = es.index(index='data-science-' + datetime.datetime.today().strftime('%Y-%m-%d'), doc_type='notebook', id=1,
               body=es_payload)
print(res['result'])