# !/usr/bin/env python
# coding=UTF-8
import numpy as np
from elasticsearch import Elasticsearch
import json
from datetime import datetime
import timedelta

# Process hits here
def process_hits(hits):
    for item in hits:
        print(json.dumps(item, indent=2))


es = Elasticsearch(['http://143.54.13.128:9200/'])

doc = {
    "query": {
        "bool": {
            "must_not": [{
                "exists": {
                    "field": "likes"
                }
            },

                {
                    "exists": {
                        "field": "replies"
                    }
                },
                {
                    "exists": {
                        "field": "retweets"
                    }
                }],
            "should": [
                {"range": {"date": {"from": "2017-01-01 00:00:00", "to": "2018-01-25 00:00:00"}}}
            ]
        }
    }
}

data = es.search(index='twint', doc_type='items', body=doc, scroll='2m')
count = 0
sid = data['_scroll_id']
scroll_size = len(data['hits']['hits'])

# Before scroll, process current batch of hits
# process_hits(data['hits']['hits'])


pos = np.zeros(32, dtype=int)
neg = np.zeros(32, dtype=int)
neu = np.zeros(32, dtype=int)




while scroll_size > 0:
    "Scrolling..."
    data = es.scroll(scroll_id=sid, scroll='2m')

    # Process current batch of hits
    # print("# ", data['hits']['hits'][0]['_source'])

    # print(data)
    if len(data['hits']['hits']) > 0:
        dataDict = data['hits']['hits'][0]['_source']
        date = datetime.strptime(dataDict['date'], '%Y-%m-%d %H:%M:%S')

        # print("##", date.day)


        if dataDict['polarity'] == 'pos':
            pos[date.day] += 1
        elif dataDict['polarity'] == 'neg':
            neg[date.day] += 1
        else:
            neu[date.day] += 1
        # lista2.append(data['hits']['hits'][0]['_source'])

        # Update the scroll ID
        sid = data['_scroll_id']

        # Get the number of results that returned in the last scroll
    scroll_size = len(data['hits']['hits'])


print("pos:", pos)
print("neg:", neg)
print("neu:", neu)

soma = []

for i in range(32):
    soma[i] = pos[i] + neg[i] + neu[i]
    pos[i] = pos[i]/soma[i]
    neg[i] = neg[i]/soma[i]
    neu[i] = neu[i]/soma[i]


print("soma", soma)
print("pos:", pos)
print("neg:", neg)
print("neu:", neu)