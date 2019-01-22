import time

import requests
import pprint

class GeoCode(object):

    def __init__(self):
        self.G_MAPS_GEOCODE_API_URL = 'https://maps.googleapis.com/maps/api/geocode/json'
        self.G_MAPS_SNAPTOROADS_API_URL = 'https://roads.googleapis.com/v1/snapToRoads'
        self.key = 'AIzaSyA7RrsTwaAPJCeKwn6EL8MwM9kgWGb4AmE'

        self.OSM_GEOCODE_API_URL = 'https://nominatim.openstreetmap.org/search'
        self.OSM_GEOCODE_REVERSE_API_URL = ' https://nominatim.openstreetmap.org/reverse'

    def OSM_address2coordinates(self, query='', street=False, city='Porto Alegre', state='RS', country='BR',
                               postalcode=False):

        params = {
            'format': 'jsonv2',
            'accept-language': 'br',
            'polygon_geojson': 1,
            'limit': 3,
            'q': query
        }

        if street: params['q'] += ', ' + str(street)
        if city: params['q'] += ', ' + str(city)
        if state: params['q'] += ', ' + str(state)
        if country: params['q'] += ', ' + str(country)
        if postalcode: params['q'] += ', ' + str(postalcode)


        print("Q: " , params['q'])

        req = requests.get(self.OSM_GEOCODE_API_URL, params=params)
        res = req.json()


        if not (type(res) is list):
            res = [res]


        listGeodata = []

        if len(res) == 0:
            print('GOOGLE MAPS API - NÃO ENCONTRADO NO OSM')
            lg = self.GMAPS_place2coordinates(query)
            for g in lg:
                listGeodata.extend(self.OSM_coordinate2address(g['lat'], g['lng']))

        elif (res[0]['category'] not in ['highway', 'boundary', 'place']):
            print('GOOGLE MAPS API - AUMENTAR PRECISÃO')
            lg = self.GMAPS_place2coordinates(query)
            for g in lg:
                listGeodata.extend(self.OSM_coordinate2address(g['lat'], g['lng']))

        else:
            print('OPEN STREET MAPS - DIRETO')
            for r in res:
                geodata = dict()
                geodata['address'] = r['display_name']
                geodata['type'] = r['geojson']['type']
                geodata['coordinates'] = r['geojson']['coordinates']

                listGeodata.append(geodata)


        # pprint.pprint(listGeodata)
        return listGeodata

    def OSM_coordinate2address(self, lat, lon):

        params = {
            'format': 'jsonv2',
            'accept-language': 'br',
            'polygon_geojson': 1,
            'addressdetails:': 1,
            'lat': lat,
            'lon': lon
        }

        req = requests.get(self.OSM_GEOCODE_REVERSE_API_URL, params=params)
        res = req.json()

        if not (type(res) is list):
            res = [res]

        # if len(res) == 0:
        #     return []


        listGeodata = []

        for r in res:
            geodata = dict()
            geodata['address'] = r['display_name']
            if ('geojson' in r.keys()):
                geodata['type'] = r['geojson']['type']
                geodata['coordinates'] = r['geojson']['coordinates']
                listGeodata.append(geodata)
            else:
                geodata['type'] = 'Point'
                geodata['coordinates'] = [lon, lat]
                listGeodata.append(geodata)

        # print(listGeodata)
        return listGeodata

    def GMAPS_coordinate2address(self, coordinate):

        params = {
            'latlng': coordinate,
            'region': 'br',
            'key': self.key
        }

        req = requests.get(self.G_MAPS_GEOCODE_API_URL, params=params)
        res = req.json()

        listGeodata = []

        if (res['status'] != 'OK'):
            for result in res['results']:
                geodata = dict()
                geodata['lat'] = result['geometry']['location']['lat']
                geodata['lng'] = result['geometry']['location']['lng']
                geodata['latlng'] = str(geodata['lat']) + ',' + str(geodata['lng'])
                geodata['address'] = result['address_components'][2]['long_name']
                listGeodata.append(geodata)
            return listGeodata

    def GMAPS_address2coordinates(self, addressList, region='Porto Alegre'):

        address = ''

        if (len(addressList) == 1):
            address = addressList[0] if region in addressList[0] \
                else addressList[0] + ',' + region
        elif (len(addressList) == 2):
            address = '{a[0]} & {a[1]}'.format(a=addressList)

        params = {
            'address': address,
            'region': 'br',
            'key': self.key
        }

        req = requests.get(self.G_MAPS_GEOCODE_API_URL, params=params)
        res = req.json()

        listGeodata = []

        if (res['status'] == 'OK'):
            for result in res['results']:
                geodata = dict()
                geodata['lat'] = result['geometry']['location']['lat']
                geodata['lng'] = result['geometry']['location']['lng']
                geodata['latlng'] = str(geodata['lat']) + ',' + str(geodata['lng'])
                geodata['vlatlng'] = [geodata['lat'], geodata['lng']]
                geodata['address'] = result['formatted_address']
                listGeodata.append(geodata)
            # print('{address}. (lat, lng) = ({lat}, {lng})'.format(**geodata))
            return listGeodata

    def GMAPS_place2coordinates(self, place, region='Porto Alegre'):

        placeRegion = place + ',' + region
        params = {
            'address': placeRegion,
            'region': 'br',
            'key': self.key
        }

        # Do the request and get the response data
        req = requests.get(self.G_MAPS_GEOCODE_API_URL, params=params)
        res = req.json()

        listGeodata = []

        if (res['status'] == 'OK'):
            for result in res['results']:
                geodata = dict()
                geodata['lat'] = result['geometry']['location']['lat']
                geodata['lng'] = result['geometry']['location']['lng']
                geodata['latlng'] = str(geodata['lat']) + ',' + str(geodata['lng'])
                geodata['vlatlng'] = [geodata['lat'], geodata['lng']]
                geodata['address'] = result['formatted_address']
                listGeodata.append(geodata)
            # print('{address}. (lat, lng) = ({lat}, {lng})'.format(**geodata))
            return listGeodata

    # def GMAPS_street2listCoordinates(self, streetName, points=20):
    #     time.sleep(0.005)
    #     pCoord = []
    #     placeRegion = ''
    #
    #     n = 1
    #     while n < 1000:
    #         n += 1000 / points
    #         a = self.address2coordinates([streetName + ',' + str(n)])
    #         pCoord.append(a)
    #
    #     for pc in pCoord:
    #         if placeRegion == '':
    #             placeRegion = '{}'.format(pc['latlng'])
    #         else:
    #             placeRegion = '{} | {}'.format(placeRegion, pc['latlng'])
    #
    #     params = {
    #         'path': placeRegion,
    #         'interpolate': True,
    #         'key': self.key
    #     }
    #
    #     req = requests.get(self.G_MAPS_SNAPTOROADS_API_URL, params=params)
    #
    #     if (req.status_code == 200):
    #         res = req.json()
    #
    #         result = res['snappedPoints']
    #
    #         listC = []
    #         for r in result:
    #             geodata = dict()
    #             geodata['lat'] = r['location']['latitude']
    #             geodata['lng'] = r['location']['longitude']
    #             geodata['latlng'] = str(geodata['lat']) + ',' + str(geodata['lng'])
    #             listC.append(geodata)
    #
    #         return listC

    # def GMAPS_imageView(self, listCoordinates, street=False):
    #     URI = 'https://maps.googleapis.com/maps/api/staticmap'
    #     markers = []
    #
    #     if (street):
    #         center = street;
    #     else:
    #         center = listCoordinates[0]['latlng']
    #
    #     for i, pc in enumerate(listCoordinates):
    #         markers.append('color:{}|label:{}|{}'.format('blue', i, pc['latlng']))
    #
    #     params = {
    #         'center': center,
    #         'zoom': '15.5',
    #         'size': '1200x720',
    #         'maptype': 'roadmap',
    #         'markers': markers,
    #         'key': self.key
    #     }
    #
    #     req = requests.get(URI, params=params)
    #
    #     return req.url

