import collections
from datetime import datetime as dt
from datetime import timedelta as td
from pprint import pprint
from random import randint, random

import numpy as np
from tqdm import tqdm


def mongo(collection_name):
    from pymongo import MongoClient
    db = MongoClient('gauss', 27017)
    collection = db.database[collection_name]

    return collection


def create(tweet, i, grid):
    # print("Create:", tweet['id_twitter'])
    new_tweet = dict()

    new_tweet['ic_id'] = i
    new_tweet['id'] = tweet['id_old']
    new_tweet['datetime'] = tweet['datetime']
    new_tweet['day'] = new_tweet['datetime'].date().day
    new_tweet['month'] = new_tweet['datetime'].date().month
    new_tweet['year'] = new_tweet['datetime'].date().year
    new_tweet['weekday'] = new_tweet['datetime'].date().weekday()
    new_tweet['hour'] = new_tweet['datetime'].time().hour
    new_tweet['tags'] = tweet['tag']

    grids = list()
    count = 0
    for local in tweet['grid_' + str(grid)]:
        grids.append((local[0], local[1]))
        # count += 1
        # if count == 2:
        #     break

    new_tweet['local'] = list(set(grids))
    new_tweet['locals'] = tweet['locals']

    new_tweet['eventos'] = evento(new_tweet['datetime'], new_tweet['local'], grid)
    new_tweet['obras'] = obra(new_tweet['datetime'], new_tweet['local'])
    new_tweet['manifestacoes'] = manifestacao(new_tweet['datetime'], new_tweet['local'])
    new_tweet['weather'] = weather(new_tweet['datetime'])
    new_tweet['holiday'] = holiday(new_tweet['datetime'])

    return new_tweet


def round_hour(datetime):
    return dt(year=datetime.date().year, month=datetime.date().month, day=datetime.date().day,
              hour=datetime.time().hour)


def round_day(datetime):
    return dt(year=datetime.date().year, month=datetime.date().month, day=datetime.date().day)


def evento(datetime, locais, grid):
    eventos_collection = mongo('eventos')
    eventos_cursor = eventos_collection.find({'$and': [{'start_date': {'$lte': round_hour(datetime)}},
                                                       {'end_date': {'$gte': round_hour(datetime)}},
                                                       {'pessoas': {'$gte': 1000}}]}, no_cursor_timeout=True)
    with_eventos = list()

    grids = list()
    for cursor in eventos_cursor:
        # print(cursor['_id'])
        for i in cursor['grid_' + str(grid)]:
            grids.append((i[0], i[1]))

        with_eventos.extend(list(set(locais).intersection(grids)))

    return list(set(with_eventos))


def obra(datetime, locais):
    obra_collection = mongo('obras_2')
    obra_cursor = obra_collection.find({'$and': [{'datetime': {'$gte': round_hour(datetime)}},
                                                 {'datetime': {'$lte': round_hour(datetime) + td(hours=1)}}]},
                                       no_cursor_timeout=True)
    with_obras = list()
    without_obras = list()

    grids = list()
    for cursor in obra_cursor:
        for i in cursor['grid_' + str(grid)]:
            grids.append((i[0], i[1]))

        with_obras.extend(list(set(locais).intersection(grids)))
        # without_obras.extend(list(set(locais).difference(with_obras)))

    return list(set(with_obras))


def manifestacao(datetime, locais):
    manifestacao_collection = mongo('manifestacao_2')
    manifestacao_cursor = manifestacao_collection.find({'$and': [{'datetime': {'$gte': round_hour(datetime)}},
                                                                 {'datetime': {
                                                                     '$lte': round_hour(datetime) + td(hours=1)}}]},
                                                       no_cursor_timeout=True)
    with_manifestacao = list()
    without_manifestacao = list()

    grids = list()
    for cursor in manifestacao_cursor:
        for i in cursor['grid_' + str(grid)]:
            grids.append((i[0], i[1]))

        with_manifestacao.extend(list(set(locais).intersection(grids)))
        # without_manifestacao.extend(list(set(locais).difference(with_manifestacao)))

    return list(set(with_manifestacao))


def weather(datetime):
    weather_collection = mongo('weather')
    weather_cursor = weather_collection.find_one({'$and': [{'datetime': {'$gte': round_day(datetime)}},
                                                           {'datetime': {'$lt': round_hour(datetime) + td(hours=1)}}]},
                                                 no_cursor_timeout=True)

    del weather_cursor['_id'], weather_cursor['datetime']
    return weather_cursor


def holiday(datetime):
    holiday_collection = mongo('holiday')
    holiday_cursor = holiday_collection.find_one({'datetime': {'$eq': round_day(datetime)}},
                                                 no_cursor_timeout=True)

    if not holiday_cursor == None:
        holiday = {'holiday': 'Feriado' == holiday_cursor['type'],
                   'holiday_eve': 'Véspera de Feriado' == holiday_cursor['type']
                   }
    else:
        holiday = {'holiday': False,
                   'holiday_eve': False
                   }
    return holiday


def dict2csv(dict_list, k, filename):
    import random
    import csv
    print("EMBARALHA LISTA")
    random.shuffle(dict_list)
    print("ESCREVENDO CSV")

    dataset = list()

    for i, dic in enumerate(dict_list):
        list_tweet_line_one_twitter = list()
        # if dic['weekday'] in [4]:                                       # Apenas sexta (4)
        # if dic['weekday'] in [5, 6] or dic['holiday']['holiday']:       # Apenas sab, dom e fer (5, 6) e fer
        if True:                                                          # Preguiça de identar
            for l in dic['local']:
                line = dict()
                # line['id'] = dic['ic_id']
                # line['datetime'] = dic['datetime']
                line['k'] = i % k
                line['day_of_week'] = dic['weekday']
                line['hour'] = dic['hour']
                line['grid_w'] = l[0]
                line['grid_h'] = l[1]
                line['evento'] = int(l in dic['eventos'])
                line['obras'] = int(l in dic['obras'])
                line['manifestacoes'] = int(l in dic['manifestacoes'])
                line['holiday'] = int(dic['holiday']['holiday'])
                line['holiday_eve'] = int(dic['holiday']['holiday_eve'])
                line.update(dic['weather'])

                line['cOutros'] = int("Outros" in dic['tags'])
                line['cObstrução'] = int("Obstrução" in dic['tags'])
                line['cTráfego_Pesado'] = int("Tráfego Pesado" in dic['tags'])
                line['cTráfego_Livre'] = int("Tráfego Livre" in dic['tags'])
                line['cIncidentes'] = int("Incidentes" in dic['tags'])
                line['cSemáforo'] = int("Semáforo" in dic['tags'])
                line['cClima'] = int("Clima" in dic['tags'])

                list_tweet_line_one_twitter.append(line)

        dataset.extend(list_tweet_line_one_twitter)


    labels = ['k', 'cOutros', 'cObstrução', 'cTráfego_Pesado', 'cTráfego_Livre', 'cIncidentes', 'cSemáforo', 'cClima',
    # labels = ['k', 'cObstrução', 'cTráfego_Pesado', 'cTráfego_Livre', 'cIncidentes',
              'day_of_week', 'hour', 'grid_w', 'grid_h', 'evento', 'obras', 'manifestacoes',
              'holiday', 'holiday_eve', 'humidity', 'cloud', 'tempC', 'windHph', 'pressureIn', 'precipIn',
              'condition']

    labels_types = [list(range(k)), [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1],
    # labels_types = [list(range(k)), [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], 'NUMERIC',
                    'NUMERIC', 'NUMERIC', 'NUMERIC', 'NUMERIC', [0, 1], [0,1], [0,1],
                    [0,1], [0,1],'NUMERIC', 'NUMERIC', 'NUMERIC', 'NUMERIC', 'NUMERIC', 'NUMERIC',
                    [1000, 1003, 1006, 1183, 1153, 1240, 1087, 1009, 1063, 1186]]






    print(labels_types)

    #####  CSV
    with open('datasets/' + filename, 'w') as f:
        w = csv.DictWriter(f, labels)
        w.writeheader()
        w.writerows(dataset)

    f.close()

    #####  Pandas

    import pandas as pd

    Y = pd.DataFrame()

    dataframe = pd.DataFrame(dataset)


    for i, l in enumerate(dataframe.columns[:7]):
        print(l)
        Y.insert(i, l, dataframe.pop(l))

    dataframe.pop('k')
    X = dataframe.copy()

    print(X.max())

    with open('datasets/' + filename.replace('dataset', 'describe').replace('.csv', '.txt'), 'a') as f:
        for x in X.describe():
            print(X.describe()[x], file=f)
        for y in Y.describe():
            print(Y.describe()[y], file=f)

    f.close()

    return X, Y


def matriz(dados, x=True):
    a = list()
    b = list()
    c = list()
    for i, t in enumerate(dados):
        if x:
            for j, v in enumerate(list(t.values())[:16]):
                a.append(i)
                b.append(j)
                c.append(v)
        else:
            for j, v in enumerate(list(t.values())[16:]):
                if v:
                    a.append(i)
                    b.append(j)
                    c.append(v)

    return sparse.lil_matrix((c, (a, b)), dtype=float)


def featureSelectionExtraTreesClassifier (X, y):
    from sklearn.ensemble import ExtraTreesClassifier
    from sklearn.datasets import load_iris
    from sklearn.feature_selection import SelectFromModel
    # iris = load_iris()
    # X, y = iris.data, iris.target
    print(X.columns)
    for i, label in enumerate(y.columns):
        clf = ExtraTreesClassifier(n_estimators=150)
        clf = clf.fit(X, y[label])

        print(label)
        for j, c in enumerate(clf.feature_importances_):
            print(c)

        print("*"*50)
        model = SelectFromModel(clf, prefit=True)
        X_new = model.transform(X)

        # print(X_new)


def featureSelection(X, Y):
    from sklearn.feature_selection import chi2, SelectKBest
    import pandas as pd
    import csv


    print(X.columns)
    for i, label in enumerate(Y.columns):

        selector = SelectKBest(chi2, k='all')
        selector.fit(X, Y[label])

        print(label)
        for i, s in enumerate(selector.scores_):
            print(s)
        print("*"*100)

def k_means(X, Y):
    from sklearn.cluster import KMeans
    kmeans = KMeans(n_clusters=4, random_state=0).fit(X)

    print(kmeans.cluster_centers_)
    print(kmeans.labels_)

def miniBatchKMeans(X, Y):
    from sklearn.cluster import MiniBatchKMeans
    mbk = MiniBatchKMeans(init='k-means++', n_clusters=4, batch_size=45,
                          n_init=10, max_no_improvement=10, verbose=0).fit(X)

    print(mbk.cluster_centers_)
    print(mbk.labels_)

    # for i, l in enumerate(mbk.labels_):
    #     print(Y.iloc[[i]], l)

def print_describe(title, val, names = None):

    print("### ", title)
    print("#" * 50)
    for i, v in enumerate(val):
        if not names == None:
            print(names[i], v)
        else:
            print(v)
    print("#" * 50)


def describe_contexts(list_context, filename, cache = True):
    import pandas as pd
    list_context = list(list_context)

    dataset = list()
    if not cache:
        for i, dic in enumerate(list_context):
            print(dic)
            list_tweet_line_one_twitter = list()
            # if dic['weekday'] in [4]:                                       # Apenas sexta (4)
            # if dic['weekday'] in [5, 6] or dic['holiday']['holiday']:       # Apenas sab, dom e fer (5, 6) e fer
            if True:                                                          # Preguiça de identar
                for l in dic['local']:
                    if l in dic['eventos'] or l in dic['obras'] or l in dic['manifestacoes'] or dic['holiday'][
                        'holiday'] or dic['holiday']['holiday_eve']:
                        line = dict()
                        line['id'] = dic['ic_id']
                        # line['datetime'] = dic['datetime']
                        line['k'] = i % k
                        line['day_of_week'] = dic['weekday']
                        line['hour'] = dic['hour']
                        line['grid_w'] = l[0]
                        line['grid_h'] = l[1]
                        line['evento'] = int(l in dic['eventos'])
                        line['obras'] = int(l in dic['obras'])
                        line['manifestacoes'] = int(l in dic['manifestacoes'])
                        line['holiday'] = int(dic['holiday']['holiday'])
                        line['holiday_eve'] = int(dic['holiday']['holiday_eve'])
                        line.update(dic['weather'])

                        line['cOutros'] = int("Outros" in dic['tags'])
                        line['cObstrução'] = int("Obstrução" in dic['tags'])
                        line['cTráfego_Pesado'] = int("Tráfego Pesado" in dic['tags'])
                        line['cTráfego_Livre'] = int("Tráfego Livre" in dic['tags'])
                        line['cIncidentes'] = int("Incidentes" in dic['tags'])
                        line['cSemáforo'] = int("Semáforo" in dic['tags'])
                        line['cClima'] = int("Clima" in dic['tags'])

                        list_tweet_line_one_twitter.append(line)

            dataset.extend(list_tweet_line_one_twitter)

        dataframe = pd.DataFrame(dataset)


        with open('describe/' + filename.replace('dataset', 'pickle').replace('.csv', '.pickle'), 'wb') as f:
            pickle.dump(dataframe, f)
            f.close()
    else:
        with open('describe/' + filename.replace('dataset', 'pickle').replace('.csv', '.pickle'), 'rb') as f:
            dataframe = pickle.load(f)
            f.close()



    print_describe("TWEETS", [len(list_context),
                              len(dataframe.groupby('id'))],
                   ["Número total de tweets", "Número de tweets com localização"])

    print_describe("CONTEXTO", [len(dataframe),
                                len(dataframe)/len(dataframe.groupby('id'))],
                   ["Número de contextos", "Média de contexto por tweet"])


    print_describe("DATA E HORA", [dataframe[['id', 'day_of_week']].groupby('day_of_week').count(),
                                       dataframe[['id', 'hour']].groupby('hour').count()])


    print_describe("EVENTOS", [dataframe[['id', 'evento']].groupby('evento').count()])
    print_describe("OBRAS", [dataframe[['id', 'obras']].groupby('obras').count()])
    print_describe("MANIFESTAÇÕES", [dataframe[['id', 'manifestacoes']].groupby('manifestacoes').count()])
    print_describe("FERIADOS", [dataframe[['id', 'holiday']].groupby('holiday').count()])
    print_describe("VESPERA DE FERIADOS", [dataframe[['id', 'holiday_eve']].groupby('holiday_eve').count()])

    # context = dataframe[dataframe['evento'] == 1]
    #
    # print_describe("DATA E HORA", [context[['id', 'day_of_week']].groupby('day_of_week').count(),
    #                                    context[['id', 'hour']].groupby('hour').count()])
    #
    # context = dataframe[dataframe['obras'] == 1]
    #
    # print_describe("DATA E HORA", [context[['id', 'day_of_week']].groupby('day_of_week').count(),
    #                                    context[['id', 'hour']].groupby('hour').count()])
    #
    # context = dataframe[dataframe['manifestacoes'] == 1]
    #
    # print_describe("DATA E HORA", [context[['id', 'day_of_week']].groupby('day_of_week').count(),
    #                                    context[['id', 'hour']].groupby('hour').count()])



if __name__ == '__main__':
    from scipy import sparse
    import pickle

    # holiday(dt(year=2018, month=12, day=23, hour=12, minute=10, second=50))
    # TODO Buscar todos Tweets
    tweet_collection = mongo('tweet')
    tweet_cursor = tweet_collection.find({}, no_cursor_timeout=True)

    list_context = list()

    dataset_id = "90"
    grid = 500
    k = 1


    filename = "dataset___id-%s__k-%s__grid-%s.csv" % (dataset_id, str(k), str(grid))


    cache = False
    if not cache:
        with tqdm(total=tweet_cursor.count()) as pbar:
            pbar.set_description("Make Dataset")

            for i, cursor in enumerate(tweet_cursor):
                # if len(list(set(cursor['tag']).difference(['Outros', 'Semáforos', 'Clima']))) > 0:
                if True:
                    context = create(cursor, i, grid)
                    list_context.append(context)
                pbar.update(1)



    describe_contexts(list_context, filename, cache)





    # X, Y = dict2csv(list_context, k, filename)

    # with open('datasets/' + filename.replace('dataset', 'pickle').replace('.csv', '.pickle'), 'wb') as f:
    #     pickle.dump((X, Y), f)
    #     f.close()
    #
    # with open('datasets/' + filename.replace('dataset', 'pickle').replace('.csv', '.pickle'), 'rb') as f:
    #     X, Y = pickle.load(f)
    #     f.close()
    # print(X)
    # k_means(X, Y)
    # miniBatchKMeans(X, Y)
    # featureSelectionExtraTreesClassifier(X, Y)


