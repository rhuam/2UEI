# !/usr/bin/env python
# coding=UTF-8
from elasticsearch import Elasticsearch
import json


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
                {"range": {"date": {"from": "2018-06-10 00:00:00", "to": "2018-06-18 00:00:00"}}}
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

print(data['hits']['hits'][1]['_source'])

while scroll_size > 0:
    "Scrolling..."
    data = es.scroll(scroll_id=sid, scroll='2m')

    # Process current batch of hits
    print(data['hits']['hits'][0]['_source'])

    # Update the scroll ID
    sid = data['_scroll_id']

    # Get the number of results that returned in the last scroll
    scroll_size = len(data['hits']['hits'])