from elasticsearch import Elasticsearch
import json
import pandas as pd

es = Elasticsearch('http://localhost:9200')

print(es.indices.get_alias("*"))

# print(es.search(index="sftp-files-*", body={'query': {"multi_match" : {
#     "query":    "banana",
#     "fields": [ "image", "filename", "actualtext", "topics", "uniqueObjects" ]
# }}}))

# search_query = {
#     "_source": [ "image", "filename", "actualtext", "topics.keyword" ],
#     'query': {"multi_match" : {
#         "query":    "car",
#         "fields": [ "image", "filename", "actualtext", "topics" ]
#     }},
#     "aggs" : {
#         "uniq_topics" : {
#             "terms" : {"field" : "topics.keyword"}
#         },
#         "uniq_actualtext" : {
#             "terms" : {"field" : "actualtext.keyword"}
#         },
#         "uniq_filename" : {
#             "terms" : {"field" : "filename.keyword"}
#         }
#     }
# }

search_query = {
    "_source": [ "image", "filename", "actualtext", "topics.keyword", "uniqueObjects" ],
    'query': {"multi_match" : {
            "query":    "bus",
            "fields": [ "image", "filename", "actualtext", "topics", "uniqueObjects" ]
        }},
    "aggs" : {
        "uniq_topics" : {
            "terms" : {"field" : "topics.keyword"}
        }
    }
}

# print(es.search(index="sftp-files-*",  filter_path=['hits.hits._source', 'hits.hits.topics.keyword', "hits.hits.image",
#                                                     "hits.hits.filename", "hits.hits.actualtext", "hits.hits.topics"],
#                 body=json.dumps(search_query)))

search_result = es.search(index="*", body=json.dumps(search_query))

print(search_result)
print(type(search_result))

sr_time_taken = search_result['took']

print(sr_time_taken)

sr_topics = search_result['aggregations']['uniq_topics']['buckets']

print(sr_topics)

sr_hits = search_result['hits']

print(sr_hits)

sr_hits_count = sr_hits['total']

print(sr_hits_count)

sr_hits_max_score = sr_hits['max_score']

print(sr_hits_max_score)

sr_hits_hits = sr_hits['hits']

print(sr_hits_hits)

remove_keys = ['_index', '_type', '_id']

for dicts in sr_hits_hits:
    for k in remove_keys:
        del dicts[k]

print(sr_hits_hits)


def parse_dict(init, lkey=''):
    ret = {}
    for rkey,val in init.items():
        key = lkey+rkey
        if isinstance(val, dict):
            ret.update(parse_dict(val, key+'_'))
        else:
            ret[key] = val
    return ret


tmp_dict = {}
sr_hits_hits_unique = []
for d in sr_hits_hits:
    sr_hits_hits_flatten = parse_dict(d, '')
    #print(sr_hits_hits_flatten)
    if tmp_dict != sr_hits_hits_flatten:
        sr_hits_hits_unique.append(sr_hits_hits_flatten)
        tmp_dict = sr_hits_hits_flatten

#print(sr_hits_hits_unique)

pd_df_grouped = pd.DataFrame(sr_hits_hits_unique).sort_values('_score').groupby('_source_filename').last()

print(pd_df_grouped.to_dict(orient='records'))
