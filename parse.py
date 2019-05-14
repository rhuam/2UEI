import csv
from datetime import datetime


def mongo(collection_name):
    from pymongo import MongoClient
    db = MongoClient('gauss', 27017)
    collection = db.database[collection_name]

    return collection

base = mongo('holiday')

with open('feriados.csv', 'r') as infile:
    reader = csv.reader(infile)
    list_tweets = list()

    for r in reader:
        feriado = dict()
        feriado = {'datetime': datetime.strptime(r[0], '%d/%m/%y'),
                   'name': r[1],
                   'type': r[2]}
        print(feriado)
        base.insert_one(feriado)




# import pprint as pprint
#
# from bson import ObjectId
#
# from dict2mongo import dict2mongo
# import pprint
#
# ### Database Connection
# context = dict2mongo('context')
# twint = dict2mongo('twint')
# tl = dict2mongo('trafficSignals')
#
# # context_bkp = dict2mongo('context_bkp')
# # twint_bkp = dict2mongo('twint_bkp')
#
# ####  DEFINE Context Time Frame ####
# ## Clima: 0 hrs
# ## Eventos: 1 hr
# ## Jogo: 2 hrs
# ## Obras: 0 hr
# ####################################
#
# # TF_CLIMA = 0
# # TF_EVENTO = 1
# # TF_JOGO = 2
# # TF_OBRAS = 0
# #
# TF = {
#     'jogo': 2,
#     'evento': 1,
#     'obras': 0,
#     'clima': 0
# }
#
# # tl.traffic_signals('/home/gauss/rsestevam/Desktop/SUMO POA/map.xml')
#
#
# from datetime import datetime
# import timedelta
#
#
# # def simpleQuery(self, bd, typeCoordinates='Point', coordinates=[-51.21333, -30.03459], limit=10,
# #                 startDate=datetime(2018, 6, 15, 0, 1, 6, 764), endDate=datetime(2018, 6, 15, 23, 55, 3, 381)):
# #     listDoc = []
# #
# #     # print(typeCoordinates, coordinates, limit, startDate, endDate)
# #
# #     cursor = bd.aggregate([{'$geoNear': {
# #         'near': {'type': typeCoordinates, 'coordinates': coordinates}, 'spherical': True,
# #         'distanceField': 'distance', 'limit': limit, 'query': {'date': {'$gte': startDate, '$lt': endDate}}}}])
# #
# #     for c in cursor:
# #         listDoc.append(c)
# #
# #     return listDoc
#
#
# # contextFile = open('twint.txt', 'w')
#
#
#
# # {
# #     coords: [{lat: 25.774, lng: -80.190},
# #     {lat: 18.466, lng: -66.118},
# #     {lat: 32.321, lng: -64.757}],
# #     contextType: 'Jogo',
# #     contextDesc: 'Final da Copa do Mundo'
# # }
#
# coordenadas = ''
#
# for c in twint.db.find({}):
#
#     pprint.pprint(c)
#
#
#     text = "{coords: [@coordenates]," \
#            "contextType: '@type'," \
#            "contextDesc: '@desc'" \
#            "}"
#
#     text = text.replace('@type', c['idOld'])
#     text = text.replace('@desc', c['tweet'] +
#                         '\n<b>Locais:<b> ' + str(c['local']))
#
#     textCo = ''
#     if 'localizacao' in c.keys():
#         for geometries in c['localizacao']['geometries']:
#
#             if ((geometries['type']) == 'Point'):
#                 pass
#
#             if ((geometries['type']) == 'MultiPolygon'):
#                 if 'localizacao' in c.keys():
#                     for coo in geometries['coordinates'][0]:
#                         for cooo in coo:
#                             textCo += '{' + ('lng: {}, lat: {}'.format(cooo[0], cooo[1])) + '}, \n'
#
#             if ((geometries['type']) == 'Polygon'):
#
#                 if 'localizacao' in c.keys():
#                     for coo in geometries['coordinates'][0]:
#                         textCo += '{' +  ('lng: {}, lat: {}'.format(coo[0], coo[1])) + '}, \n'
#
#             if ((geometries['type']) == 'LineString'):
#
#                 if 'localizacao' in c.keys():
#                     for coo in geometries['coordinates']:
#                         textCo += '{' + ('lng: {}, lat: {}'.format(coo[0], coo[1])) + '}, \n'
#
#             text = text.replace('@coordenates', textCo)
#
#             coordenadas += text + ', \n'
#
# arq = open('file.txt', 'w+')
# print(coordenadas, file=arq)
#
# # for c in context.db.find({}):
# #     if 'localizacao' in c.keys():
# #         print(c.keys())
# #         type = c['localizacao']['type']
# #         l = c['localizacao']
# #         d = c['date']
# #
# #
# #         type = ''
# #
# #         if type == 'Point':
# #             simpleQuery(twint, typeCoordinates='Point', coordinates=l['coordinates'],
# #                         startDate=d + timedelta.Timedelta(hours=TF[c['context_type']]),
# #                         endDate=d + timedelta.Timedelta(hours=TF[c['context_type']]))
# #         elif type == 'LineString':
# #             pass
# #         elif type == 'Polygon':
# #             pass
# #         elif type == 'MultiPolygon':
# #             pass
#
# # print(twint.db.count())
# # for c in twint.db.find({}):
# #     # del c['localizacao']
# #     print("---------------------------------")
# #     if 'localizacao' in c.keys():
# #         for g in c['localizacao']['geometries']:
# #             print(g['type'])
#
#
# # for c in twint.db.find({'_id': ObjectId('5bbcb70ab0a4c01be0345ff4')}):
# #     print("---------------------------------")
# #     pprint.pprint(c)
#
#
# import pprint, timedelta
#
# # context.jogoContext('dados_contexto_jogo.csv')
# # arq = open('output.txt', 'w')
# # for doc in context.db.find({}).skip(191):
# # pprint.pprint(doc, arq)
# # # pprint.pprint(doc['context_type'], arq)
# # # pprint.pprint(doc['localizacao']['address'], arq)
# # # pprint.pprint(doc['localizacao']['type'], arq)
# # pprint.pprint('-----------------------------------------', arq)
#
#
# # pprint.pprint(doc)
#
# # context.db.remove({'_id':doc['_id']})
#
# # print(doc['localizacao']['address'])
#
#
# # print(doc['localizacao'])
# #
# # #
# # if ('end_date' not in doc.keys()):
# #     doc['end_date'] = doc['date'] + timedelta.Timedelta(hours=1)
# # #
# # # # print(doc['date'], doc['end_date'])
# # #
# # #
# # listTweets = twint.testQuery(typeCoordinates=doc['localizacao']['type'], coordinates=doc['localizacao']['coordinates'],
# #                   startDate=doc['date'] + timedelta.Timedelta(hours=TF[doc['context_type']]),
# #                   endDate=doc['end_date'] + timedelta.Timedelta(hours=TF[doc['context_type']]), limit=10)
# #
# # pprint.pprint(listTweets)
# # breakpoint()
#
# # print()
#
#
# # ob.obrasContext()
#
# # ob.query()
# #
# # for doc in ob.db.find({}):
# #     print(doc)
#
#
# # for doc in twint.find({'date': {'$gte': start, '$lt': end}}):
# #     print(doc)
#
# # Apaga Tudo
# # context.delete_many({})
