from pymongo import MongoClient
from bson import json_util
from bson.objectid import ObjectId
from datetime import datetime
import csv
import geocode
import pprint


class dict2mongo(object):

    def __init__(self, db='db'):
        self.__db = MongoClient('gauss', 27017)
        self.__collection = self.__db.database[db]

    @property
    def db(self):
        return self.__collection

    def obrasContext(self, filePath):
        cd = {}
        geo = geocode.GeoCode()

        with open(filePath, mode='r') as infile:
            reader = csv.reader(infile)

            for r in reader:
                cd['_id'] = ObjectId()
                cd['context_type'] = 'obras'
                g = geo.OSM_address2coordinates(r[4])
                cd['localizacao'] = {'address': g[0]['address'],
                                     'coordinates': g[0]['coordinates'],
                                     'type': g[0]['type']}
                cd['date'] = datetime.strptime(r[0] + ' ' + r[1], '%d/%m/%y %H:%M:%S')
                cd['end_date'] = datetime.strptime(r[2] + ' ' + r[3], '%d/%m/%y %H:%M:%S')
                cd['descricao'] = r[5]
                print(cd)
                self.__collection.insert_one(cd)


    def eventosContext(self, filePath):
        cd = {}
        geo = geocode.GeoCode()

        with open(filePath, mode='r') as infile:
            reader = csv.reader(infile)

            for r in reader:
                cd['_id'] = ObjectId()
                cd['context_type'] = 'evento'
                g = geo.OSM_address2coordinates(r[3])
                cd['localizacao'] = {'address': g[0]['address'],
                                     'coordinates': g[0]['coordinates'],
                                     'type': g[0]['type']}
                cd['date'] = datetime.strptime(r[0] + ' ' + r[1], '%d/%m/%y %H:%M:%S')
                cd['end_date'] = datetime.strptime(r[0] + ' ' + r[2], '%d/%m/%y %H:%M:%S')
                cd['descricao'] = r[4]
                print(cd)
                self.__collection.insert_one(cd)

    def jogoContext(self, filePath):
        cd = {}
        geo = geocode.GeoCode()

        with open(filePath, mode='r') as infile:
            reader = csv.reader(infile)

            for r in reader:
                cd['_id'] = ObjectId()
                cd['context_type'] = 'jogo'
                cd['tipo_jogo'] = r[4]
                g = geo.OSM_address2coordinates(r[3])
                cd['localizacao'] = {'address': g[0]['address'],
                                     'coordinates': g[0]['coordinates'],
                                     'type': g[0]['type']}
                cd['date'] = datetime.strptime(r[0] + ' ' + r[1], '%d/%m/%y %H:%M:%S')
                cd['descricao'] = r[2]
                print(cd)
                self.__collection.insert_one(cd)

    def climaContext(self, filePath, city='Porto Alegre'):
        cd = {}
        geo = geocode.GeoCode()
        with open(filePath, mode='r') as infile:
            reader = csv.reader(infile)
            g = geo.OSM_address2coordinates(city=city)

            for r in reader:
                cd['_id'] = ObjectId()
                cd['context_type'] = 'clima'
                cd['date'] = datetime.strptime(str(r[0]) + ' ' + str(r[1]), '%d/%m/%y %H')
                cd['humidade'] = r[3]
                cd['temperatura'] = r[4]
                cd['description'] = r[5]
                cd['descricao'] = r[6]
                cd['localizacao'] = {'address': g[0]['address'],
                                     'coordinates': g[0]['coordinates'],
                                     'type': g[0]['type']}
                print(cd)
                self.__collection.insert_one(cd)

    def tweetContext(self, json):
        geo = geocode.GeoCode()
        data = json_util.loads(open(json).read())
        total = len(data['value'])
        i = 1
        for d in data['value']:
            print(round(i / total, 3), d['_source']['id'])

            d['_source']['idOld'] = d['_source']['id']
            del d['_source']['id']
            d['_source']['_id'] = ObjectId()
            d['_source']['date'] = datetime.strptime(d['_source']['date'], '%Y-%m-%d %H:%M:%S')

            if len(d['_source']['local']) > 0:
                d['_source']['localizacao'] = {
                    'type': 'GeometryCollection',
                    'geometries': []
                }

                for l in d['_source']['local']:
                    lg = geo.OSM_address2coordinates(l)
                    for g in lg:
                        d['_source']['localizacao']['geometries'].append(
                            {'address': g['address'],
                             'coordinates': g['coordinates'],
                             'type': g['type']}
                        )

            del d['_source']['hour']
            del d['_source']['timezone']
            del d['_source']['user_rt']
            del d['_source']['hashtags']
            del d['_source']['day']
            del d['_source']['essid']
            del d['_source']['retweet']
            del d['_source']['location']

            print(d['_source'])
            i += 1
            print('--------------------------------------------')
            self.__collection.insert_one(d['_source'])

    def simpleQuery(self, typeCoordinates='Point', coordinates=[-51.21333, -30.03459], limit=10,
                    startDate=datetime(2018, 6, 15, 0, 1, 6, 764), endDate=datetime(2018, 6, 15, 23, 55, 3, 381)):
        cursor = self.__collection.aggregate([{'$geoNear': {
            'near': {'type': typeCoordinates, 'coordinates': coordinates}, 'spherical': True,
            'distanceField': 'distance', 'limit': limit, 'query': {'date': {'$gte': startDate, '$lt': endDate}}}}])

        for c in cursor:
            print("-------------------------------------------------")
            pprint.pprint(c)

        self.__db.close()
