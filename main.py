from datetime import datetime

from bson import ObjectId
from tqdm import tqdm


def mongo(collection_name):
    from pymongo import MongoClient
    db = MongoClient('gauss', 27017)
    collection = db.database[collection_name]

    return collection


def save(list_data, collection):
    for data in list_data:
        mongo(collection).update_one({
            "_id": data['_id']
        }, {
            '$set': data
        },
            upsert=True)
        # print('Insert/Update: Ok')


def json2learningData(file="./rotulos-74d1d-export.json"):
    list_data = list()

    with open(file, mode='r') as infile:
        import json
        reader = json.load(infile)

        for r in reader.items():
            data = dict()
            data['id'] = r[1]['id']
            data['tag'] = r[1]['rotulos']
            list_data.append(data)

    return list_data

if __name__ == "__main__":
    ev = mongo("taxi_trips")
    eventos = ev.find({}, no_cursor_timeout=True)

    list_taxi = list()
    with tqdm(total=eventos.count()) as pbar:
        pbar.set_description("Criando nova base de Taxi Trips")
        for e in eventos:
            try:
                e['datetime_start'] = datetime.strptime(e['Trip Start Timestamp'], '%m/%d/%Y %H:%M:%S %p')

                if e['datetime_start'].year == 2017:
                    e['datetime_end'] = datetime.strptime(e['Trip End Timestamp'], '%m/%d/%Y %H:%M:%S %p')
                    save([e], 'taxi_trips_2017')
            except Exception as exc:
                print(str(exc))
                print(e)

            pbar.update(1)
    # l = json2learningData()


    # le = list()
    # for e in eventos:
    #     a = dict()
    #     a['_id'] = ObjectId()
    #     a['name'] = e['locals'][0]
    #     a['coordinates'] = e['localizacao']
    #     le.append(a)
    #
    #
    # save(l, 'tweet')
