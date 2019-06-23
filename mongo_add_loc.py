import collections
from pprint import pprint

import numpy


def mongo(collection_name):
    from pymongo import MongoClient
    db = MongoClient('gauss', 27017)
    collection = db.database[collection_name]

    return collection


def update(collection, dict):
    collection.replace_one({
            "_id": dict['_id']
        },
            dict
        )
    print('ok')


collections_list = ['manifestacoes', 'eventos', 'obras', 'tweets']

for c_name in collections_list:
    cursor = mongo(c_name).find({'places_grid': {'$exists': True}}, no_cursor_timeout=True)

    for c in cursor:
        list_cursor_grid = list()
        for places in c['places_grid']:
            for g in places['grid']:
                list_cursor_grid.append((g['w'], g['h']))

        c['grid_union'] = list(set(list_cursor_grid))
        update(mongo(c_name), c)


    #     list_locais_grid = list()
    #     for p in t['locais']:
    #         try:
    #             l = mongo()['locais'].find_one({'name': p}, no_cursor_timeout=True)
    #             list_locais_grid.append({'place': p, 'grid': l['grid']})
    #         except Exception:
    #             pass
    #
    #     t['places_grid'] = list_locais_grid
    #     update(col['manifestacoes'], t)


# from bson.json_util import dumps
# if __name__ == '__main__':
#     tweets = mongo()['tweets']
#     cursor = tweets.find({})
#     file = open("collection.json", "w")
#     file.write('[')
#     for document in cursor:
#         file.write(dumps(document))
#         file.write(',')
#     file.write(']')



