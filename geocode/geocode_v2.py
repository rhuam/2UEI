import pprint

from geopy.exc import GeocoderQueryError
from geopy.geocoders import Nominatim
from geopy.geocoders.base import DEFAULT_SENTINEL
from geopy.util import logger


class NominatimV2(Nominatim):
    def reverse(
            self,
            query,
            exactly_one=True,
            timeout=DEFAULT_SENTINEL,
            language=False,
            addressdetails=True,
            geometry=None
    ):
        """
        Return an address by location point.

        :param query: The coordinates for which you wish to obtain the
            closest human-readable addresses.
        :type query: :class:`geopy.point.Point`, list or tuple of ``(latitude,
            longitude)``, or string as ``"%(latitude)s, %(longitude)s"``.

        :param bool exactly_one: Return one result or a list of results, if
            available.

        :param int timeout: Time, in seconds, to wait for the geocoding service
            to respond before raising a :class:`geopy.exc.GeocoderTimedOut`
            exception. Set this only if you wish to override, on this call
            only, the value set during the geocoder's initialization.

        :param str language: Preferred language in which to return results.
            Either uses standard
            `RFC2616 <http://www.ietf.org/rfc/rfc2616.txt>`_
            accept-language string or a simple comma-separated
            list of language codes.

            .. versionadded:: 1.0.0

        :param bool addressdetails: Whether or not to include address details,
            such as city, county, state, etc. in *Location.raw*

            .. versionadded:: 1.14.0

        :rtype: ``None``, :class:`geopy.location.Location` or a list of them, if
            ``exactly_one=False``.

        """
        try:
            lat, lon = self._coerce_point_to_string(query).split(',')
        except ValueError:
            raise ValueError("Must be a coordinate pair or Point")
        params = {
            'lat': lat,
            'lon': lon,
            'format': 'json',
        }
        if language:
            params['accept-language'] = language

        params['addressdetails'] = 1 if addressdetails else 0

        if geometry is not None:
            geometry = geometry.lower()
            if geometry == 'wkt':
                params['polygon_text'] = 1
            elif geometry == 'svg':
                params['polygon_svg'] = 1
            elif geometry == 'kml':
                params['polygon_kml'] = 1
            elif geometry == 'geojson':
                params['polygon_geojson'] = 1
            else:
                raise GeocoderQueryError(
                    "Invalid geometry format. Must be one of: "
                    "wkt, svg, kml, geojson."
                )

        url = self._construct_url(self.reverse_api, params)
        logger.debug("%s.reverse: %s", self.__class__.__name__, url)

        return self._parse_json(
            self._call_geocoder(url, timeout=timeout), exactly_one
        )


class GeoCode(object):

    def __init__(self):
        self.G_MAPS_KEY = 'AIzaSyA7RrsTwaAPJCeKwn6EL8MwM9kgWGb4AmE'


    def address2coordinates(self, addressQuery, city=None, state=None, region='br'):
        query = addressQuery
        query += (', ' + str(city) if city else '')
        query += (', ' + str(state) if state else '')

        p1 = self.GMAPS_address2locations(query, region)
        p2 = self.OSM_address2locations(query, region)

        res = self.match_locations(gmaps_list=p1, osm_list=p2, max_distance=1000)

        if (len(res) < 1):
            res = self.OSM_coordinates2locations(p1)

        print("GMAPS: ", len(p1))
        pprint.pprint( p1)
        pprint.pprint( p2)
        pprint.pprint(res)

        return res

    def OSM_address2locations(self, query, region='br'):

        geolocator = NominatimV2()
        list_location = geolocator.geocode(query, exactly_one=False, geometry='geojson', language=region)

        return list_location if not list_location is None else list()

    def OSM_coordinates2locations(self, coordinates, region='br'):

        geolocator = NominatimV2()
        list_location = list()

        for c in coordinates:
            list_location.extend(geolocator.reverse(c.point, exactly_one=False, geometry='geojson', language=region))

        return list_location if not list_location is None else list()

    def GMAPS_address2locations(self, query, region='br'):
        from geopy.geocoders import GoogleV3

        geolocator = GoogleV3(api_key=self.G_MAPS_KEY)
        list_location = geolocator.geocode(query, exactly_one=False, region=region)

        return list_location if not list_location is None else list()

    def match_locations(self, gmaps_list, osm_list, max_distance=1000):
        from geopy.distance import great_circle

        list_location = list()

        for i in gmaps_list:
            for j in osm_list:
                d = great_circle(i.point, j.point).meters

                if d < max_distance:
                    list_location.append(j)
                    osm_list.remove(j)

        return list_location

    def geometry(self, list_location):
        if len(list_location) == 1:
            return list_location[0].raw['geojson']
        else:
            collection = dict()
            collection['type'] = 'GeometryCollection'
            collection['geometries'] = list()

            for location in list_location:
                collection['geometries'].append(location.raw['geojson'])

        return collection



if __name__ == "__main__":
    geo = GeoCode()

    search = "Rua Santo Antônio"

    g = geo.address2coordinates(addressQuery=search,
                                city="Porto Alegre",
                                state="rs")

    pprint.pprint(geo.geometry(g))
