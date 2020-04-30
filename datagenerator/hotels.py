import os
from random import randint, gauss, choice, random

from rdflib import Graph, URIRef, RDF, Literal, OWL, RDFS

from datagenerator import DataGenerator
from dataloader.datasampler import AREA_FEATURE_CLS, POINT_FEATURE_CLS, \
    LINE_FEATURE_CLS


class Hotel(object):
    def __init__(
            self,
            hotel_polygon,
            room_polygons,
            reception_polygon,
            parking_lot_polygon=None):

        self.hotel_polygon = hotel_polygon
        self.room_polygons = room_polygons
        self.reception_polygon = reception_polygon
        self.parking_lot_polygon = parking_lot_polygon

    def __str__(self):
        s = self.hotel_polygon + ',' + ','.join(self.room_polygons) + ',' + \
            self.reception_polygon

        if self.parking_lot_polygon is not None:
            s += ',' + self.parking_lot_polygon

        return s

    def get_iri(self):
        return URIRef(
            DataGenerator.ns + f'feature_hotel{hash(self.hotel_polygon)}')

    def to_pg_sql(self):
        ns = DataGenerator.ns

        sql_str = ''

        # hotel
        hotel_geom_iri = URIRef(ns + f'geom_hotel{hash(self.hotel_polygon)}')
        sql_str += \
            f"INSERT INTO {DataGenerator.polygon_table_name} " \
                f"VALUES (" \
                f"'{hotel_geom_iri}', " \
                f"ST_GeomFromText('{self.hotel_polygon}'));" + os.linesep

        # rooms
        for room_polygon in self.room_polygons:
            room_geom_iri = URIRef(ns + f'geom_room{hash(room_polygon)}')
            sql_str += \
                f"INSERT INTO {DataGenerator.polygon_table_name} " \
                    f"VALUES (" \
                    f"'{hotel_geom_iri}', " \
                    f"ST_GeomFromText('{room_polygon}'));" + os.linesep

        # reception
        reception_geom_iri = \
            URIRef(ns + f'geom_reception{hash(self.reception_polygon)}')
        sql_str += \
            f"INSERT INTO {DataGenerator.polygon_table_name} " \
                f"VALUES (" \
                f"'{reception_geom_iri}', " \
                f"ST_GeomFromText('{self.reception_polygon}'));" + os.linesep

        # parking_lot
        if self.parking_lot_polygon is not None:
            parking_lot_geom_iri = \
                URIRef(ns + f'geom_parking_lot{hash(self.parking_lot_polygon)}')

            sql_str += \
                f"INSERT INTO {DataGenerator.polygon_table_name} " \
                    f"VALUES (" \
                    f"'{parking_lot_geom_iri}', " \
                    f"ST_GeomFromText('{self.parking_lot_polygon}'));" + \
                os.linesep

        return sql_str

    def to_rdf(self):
        ns = DataGenerator.ns
        has_geom = DataGenerator.geosparql_has_geometry
        as_wkt = DataGenerator.geosparql_as_wkt
        wkt_dtype = DataGenerator.wkt_dtype

        hotel_cls = URIRef(ns + 'Hotel')
        reception_cls = URIRef(ns + 'Reception')
        room_cls = URIRef(ns + 'Room')
        parking_lot_cls = URIRef(ns + 'CarPark')

        has_room = URIRef(ns + 'has_room')
        has_reception = URIRef(ns + 'has_reception')
        has_parking_lot = URIRef(ns + 'has_carpark')

        g = Graph()

        # hotel
        hotel_feature_iri = \
            URIRef(ns + f'feature_hotel{hash(self.hotel_polygon)}')
        hotel_geom_iri = URIRef(ns + f'geom_hotel{hash(self.hotel_polygon)}')

        g.add((hotel_feature_iri, RDF.type, AREA_FEATURE_CLS))
        g.add((hotel_feature_iri, RDF.type, hotel_cls))
        g.add((hotel_feature_iri, has_geom, hotel_geom_iri))

        g.add((
            hotel_geom_iri,
            as_wkt,
            Literal(self.hotel_polygon, None, wkt_dtype)))

        # rooms
        for room_polygon in self.room_polygons:
            room_feature_iri = URIRef(ns + f'feature_room{hash(room_polygon)}')
            room_geom_iri = URIRef(ns + f'geom_room{hash(room_polygon)}')

            g.add((room_feature_iri, RDF.type, room_cls))
            g.add((hotel_feature_iri, has_room, room_feature_iri))

            g.add((room_feature_iri, RDF.type, AREA_FEATURE_CLS))
            g.add((room_feature_iri, has_geom, room_geom_iri))

            g.add((
                room_geom_iri,
                as_wkt,
                Literal(room_polygon, None, wkt_dtype)))

        # reception
        reception_feature_iri = \
            URIRef(ns + f'feature_reception{hash(self.reception_polygon)}')
        reception_geom_iri = \
            URIRef(ns + f'geom_reception{hash(self.reception_polygon)}')

        g.add((reception_feature_iri, RDF.type, reception_cls))
        g.add((hotel_feature_iri, has_reception, reception_feature_iri))

        g.add((reception_feature_iri, RDF.type, AREA_FEATURE_CLS))
        g.add((reception_feature_iri, has_geom, reception_geom_iri))

        g.add((
            reception_geom_iri,
            as_wkt,
            Literal(self.reception_polygon, None, wkt_dtype)))

        # parking lot
        if self.parking_lot_polygon is not None:
            parking_lot_feature_iri = URIRef(
                ns + f'feature_parking_lot{hash(self.parking_lot_polygon)}')
            parking_lot_geom_iri = URIRef(
                ns + f'geom_parking_lot{hash(self.parking_lot_polygon)}')

            g.add((parking_lot_feature_iri, RDF.type, parking_lot_cls))
            g.add((hotel_feature_iri, has_parking_lot, parking_lot_feature_iri))

            g.add((parking_lot_feature_iri, RDF.type, AREA_FEATURE_CLS))
            g.add((parking_lot_feature_iri, has_geom, parking_lot_geom_iri))

            g.add((
                parking_lot_geom_iri,
                as_wkt,
                Literal(self.parking_lot_polygon, None, wkt_dtype)))

        return g


class CarFriendlyHotelGenerator(object):
    def __init__(self, num_pos, num_neg):
        self.num_pos = num_pos
        self.num_neg = num_neg

        self._min_nr_of_rooms = 3
        self._max_nr_of_rooms = 10
        self._hotel_room_size = .0001  # all rooms are squared
        self._center_pos_lon = 13.74
        self._center_pos_lat = 51.05

        self.north = 0
        self.east = 1
        self.south = 2
        self.west = 3
        self.orientations = [self.north, self.east, self.south, self.west]

    def _get_hotel_center(self):
        return \
            gauss(self._center_pos_lon, 0.1), gauss(self._center_pos_lat, 0.1)

    def _create_hotel_polygon(self, center_lon, center_lat, side_length):
        d = side_length / 2

        points = []
        points.append((center_lon - d, center_lat + d))
        points.append((center_lon + d, center_lat + d))
        points.append((center_lon + d, center_lat - d))
        points.append((center_lon - d, center_lat - d))
        points.append((center_lon - d, center_lat + d))

        point_strings = \
            map(lambda p: f'{round(p[0], 5)} {round(p[1], 5)}', points)

        return 'POLYGON((' + ','.join(point_strings) + '))'

    def _create_hotel_room(self, room_center_lon, room_center_lat):
        d = self._hotel_room_size / 2

        points = []
        points.append((room_center_lon - d, room_center_lat + d))
        points.append((room_center_lon + d, room_center_lat + d))
        points.append((room_center_lon + d, room_center_lat - d))
        points.append((room_center_lon - d, room_center_lat - d))
        points.append((room_center_lon - d, room_center_lat + d))

        point_strings = \
            map(lambda p: f'{round(p[0], 5)} {round(p[1], 5)}', points)

        return 'POLYGON((' + ','.join(point_strings) + '))'

    def _create_hotel_room_polygons(self, hotel_lon, hotel_lat, num_rooms, orientation):
        rooms = []
        if orientation == self.north:
            lat = hotel_lat + (num_rooms / 2 * self._hotel_room_size) - \
                (self._hotel_room_size / 2)

            curr_lon = hotel_lon - (num_rooms / 2 * self._hotel_room_size) + \
                (self._hotel_room_size / 2)

            for i in range(num_rooms):
                rooms.append(self._create_hotel_room(curr_lon, lat))
                curr_lon += self._hotel_room_size

        elif orientation == self.south:
            lat = hotel_lat - (num_rooms / 2 * self._hotel_room_size) + \
                (self._hotel_room_size / 2)

            curr_lon = hotel_lon - (num_rooms / 2 * self._hotel_room_size) + \
                (self._hotel_room_size / 2)

            for i in range(num_rooms):
                rooms.append(self._create_hotel_room(curr_lon, lat))
                curr_lon += self._hotel_room_size

        elif orientation == self.east:
            lon = hotel_lon + (num_rooms / 2 * self._hotel_room_size) - \
                (self._hotel_room_size / 2)

            curr_lat = hotel_lat - (num_rooms / 2 * self._hotel_room_size) + \
                (self._hotel_room_size / 2)

            for i in range(num_rooms):
                rooms.append(self._create_hotel_room(lon, curr_lat))
                curr_lat += self._hotel_room_size

        else:
            lon = hotel_lon - (num_rooms / 2 * self._hotel_room_size) + \
                (self._hotel_room_size / 2)

            curr_lat = hotel_lat - (num_rooms / 2 * self._hotel_room_size) + \
                (self._hotel_room_size / 2)

            for i in range(num_rooms):
                rooms.append(self._create_hotel_room(lon, curr_lat))
                curr_lat += self._hotel_room_size

        return rooms

    def _create_reception_polygon(self, hotel_lon, hotel_lat, num_rooms, orientation):
        if orientation == self.north:
            lon = hotel_lon
            lat = hotel_lat + (num_rooms / 2 * self._hotel_room_size) - \
                (self._hotel_room_size / 2)

            return self._create_hotel_room(lon, lat)

        elif orientation == self.south:
            lon = hotel_lon
            lat = hotel_lat - (num_rooms / 2 * self._hotel_room_size) + \
                (self._hotel_room_size / 2)

            return self._create_hotel_room(lon, lat)

        elif orientation == self.east:
            lon = hotel_lon + (num_rooms / 2 * self._hotel_room_size) - \
                (self._hotel_room_size / 2)
            lat = hotel_lat

            return self._create_hotel_room(lon, lat)

        else:
            lon = hotel_lon - (num_rooms / 2 * self._hotel_room_size) + \
                (self._hotel_room_size / 2)
            lat = hotel_lat

            return self._create_hotel_room(lon, lat)

    def _create_parking_lot_polygon(self, hotel_lon, hotel_lat, num_rooms, orientation):
        if orientation == self.north:
            lon = hotel_lon
            lat = hotel_lat + (num_rooms*.75 * self._hotel_room_size)

        elif orientation == self.south:
            lon = hotel_lon
            lat = hotel_lat - (num_rooms*.75 * self._hotel_room_size)

        elif orientation == self.east:
            lon = hotel_lon + (num_rooms*.75 * self._hotel_room_size)
            lat = hotel_lat

        else:
            lon = hotel_lon - (num_rooms*.75 * self._hotel_room_size)
            lat = hotel_lat

        points = []
        side_len = num_rooms/2 * self._hotel_room_size
        d = side_len / 2
        points.append((lon - d, lat + d))
        points.append((lon + d, lat + d))
        points.append((lon + d, lat - d))
        points.append((lon - d, lat - d))
        points.append((lon - d, lat + d))

        point_strings = \
            map(lambda p: f'{round(p[0], 5)} {round(p[1], 5)}', points)

        return 'POLYGON((' + ','.join(point_strings) + '))'

    def _create_parking_lot_polygon_with_rand_offset(
            self, hotel_lon, hotel_lat, num_rooms, orientation):

        plus_minus = ['+', '-']
        # random offsets, but at least 1/2 of a hotel room
        offset_lon = (3 * random() * self._hotel_room_size) + \
                     (self._hotel_room_size * 0.5)
        offset_lat = (3 * random() * self._hotel_room_size) + \
                     (self._hotel_room_size * 0.5)

        if orientation == self.north:
            if choice(plus_minus) == '+':
                lon = hotel_lon + offset_lon
            else:
                lon = hotel_lon - offset_lon

            lat = hotel_lat + \
                (num_rooms*.75 * self._hotel_room_size) + offset_lat

        elif orientation == self.south:
            if choice(plus_minus) == '+':
                lon = hotel_lon + offset_lon
            else:
                lon = hotel_lon - offset_lon

            lat = hotel_lat - \
                (num_rooms*.75 * self._hotel_room_size) - offset_lat

        elif orientation == self.east:
            lon = hotel_lon + \
                (num_rooms*.75 * self._hotel_room_size) + offset_lon

            if choice(plus_minus) == '+':
                lat = hotel_lat + offset_lat
            else:
                lat = hotel_lat - offset_lat

        else:
            lon = hotel_lon - \
                (num_rooms*.75 * self._hotel_room_size) - offset_lon

            if choice(plus_minus) == '+':
                lat = hotel_lat + offset_lat
            else:
                lat = hotel_lat - offset_lat

        points = []
        side_len = num_rooms/2 * self._hotel_room_size
        d = side_len / 2
        points.append((lon - d, lat + d))
        points.append((lon + d, lat + d))
        points.append((lon + d, lat - d))
        points.append((lon - d, lat - d))
        points.append((lon - d, lat + d))

        point_strings = \
            map(lambda p: f'{round(p[0], 5)} {round(p[1], 5)}', points)

        return 'POLYGON((' + ','.join(point_strings) + '))'

    def generate_car_friendly_hotel(self):
        """
        A car-friendly hotel has to fulfill the following:
        - There should be a parking lot externally connected to the hotel
        - There should be a reception being a tangential proper part of the
          hotel
        - The reception and the parking lot should be externally connected
        """
        hotel_lon, hotel_lat = self._get_hotel_center()

        rooms_orientation = choice(self.orientations)

        # A hotel is squared with all rooms on one side
        # .------------------.
        # | R1 |             |
        # |----|             |
        # | R2 |             |
        # |----|             |
        # | R3 |             |
        # |----|             |
        # | R4 |             |
        # `------------------´

        reception_orientation = choice(self.orientations)

        while reception_orientation == rooms_orientation:
            reception_orientation = choice(self.orientations)

        # Now the reception (and thus the parking lot) is on one of the
        # remaining sides, e.g.
        # .------------------.
        # | R1 |             |
        # |----|             |
        # | R2 |             |
        # |----|             |
        # | R3 |             |
        # |----|.------.     |
        # | R4 || Recp |     |
        # `------------------´
        #    |            |
        #    |     P      |
        #    |            |
        #    `------------´

        num_rooms = randint(self._min_nr_of_rooms, self._max_nr_of_rooms)

        hotel_polygon = self._create_hotel_polygon(
            hotel_lon, hotel_lat, num_rooms * self._hotel_room_size)

        room_polygons = self._create_hotel_room_polygons(
            hotel_lon, hotel_lat, num_rooms, rooms_orientation)

        reception_polygon = self._create_reception_polygon(
            hotel_lon, hotel_lat, num_rooms, reception_orientation)

        parking_lot_polygon = self._create_parking_lot_polygon(
            hotel_lon, hotel_lat, num_rooms, reception_orientation)

        return Hotel(
            hotel_polygon,
            room_polygons,
            reception_polygon,
            parking_lot_polygon)

    def generate_not_car_friendly_hotel(self):
        """
        A hotel is considered not car friendly if:
        a) There is no parking lot, or
        b) there is a parking lot which is disconnected from the hotel, or
        c) there is a parking lot externally connected to the hotel, but not
          externally connected to the reception
        """
        settings = ['a)', 'b)', 'c)']

        this_setting = choice(settings)

        hotel_lon, hotel_lat = self._get_hotel_center()

        rooms_orientation = choice(self.orientations)

        # A hotel is squared with all rooms on one side
        # .------------------.
        # | R1 |             |
        # |----|             |
        # | R2 |             |
        # |----|             |
        # | R3 |             |
        # |----|             |
        # | R4 |             |
        # `------------------´

        reception_orientation = choice(self.orientations)

        while reception_orientation == rooms_orientation:
            reception_orientation = choice(self.orientations)

        # Now the reception is on one of the
        # remaining sides, e.g.
        # .------------------.
        # | R1 |             |
        # |----|             |
        # | R2 |             |
        # |----|             |
        # | R3 |             |
        # |----|.------.     |
        # | R4 || Recp |     |
        # `------------------´

        num_rooms = randint(self._min_nr_of_rooms, self._max_nr_of_rooms)

        hotel_polygon = self._create_hotel_polygon(
            hotel_lon, hotel_lat, num_rooms * self._hotel_room_size)

        room_polygons = self._create_hotel_room_polygons(
            hotel_lon, hotel_lat, num_rooms, rooms_orientation)

        reception_polygon = self._create_reception_polygon(
            hotel_lon, hotel_lat, num_rooms, reception_orientation)

        if this_setting == 'a)':
            # then we're done
            return Hotel(
                hotel_polygon,
                room_polygons,
                reception_polygon)

        elif this_setting == 'b)':
            parking_lot_orientation = choice(self.orientations)

            parking_lot_polygon = \
                self._create_parking_lot_polygon_with_rand_offset(
                    hotel_lon, hotel_lat, num_rooms, parking_lot_orientation)

            return Hotel(
                hotel_polygon,
                room_polygons,
                reception_polygon,
                parking_lot_polygon)

        elif this_setting == 'c)':
            parking_lot_orientation = choice(self.orientations)

            while parking_lot_orientation == reception_orientation or \
                    parking_lot_orientation == rooms_orientation:

                parking_lot_orientation = choice(self.orientations)

            parking_lot_polygon = self._create_parking_lot_polygon(
                hotel_lon, hotel_lat, num_rooms, parking_lot_orientation)

            return Hotel(
                hotel_polygon,
                room_polygons,
                reception_polygon,
                parking_lot_polygon)

    def get_hotels_ontology(self):
        g = Graph()
        spatial_feature_cls = URIRef(
            'http://dl-learner.org/spatial#SpatialFeature')

        g.add((
            DataGenerator.geosparql_has_geometry,
            RDF.type,
            OWL.ObjectProperty))

        g.add((DataGenerator.geosparql_as_wkt, RDF.type, OWL.DatatypeProperty))
        g.add((POINT_FEATURE_CLS, RDF.type, OWL.Class))
        g.add((LINE_FEATURE_CLS, RDF.type, OWL.Class))
        g.add((AREA_FEATURE_CLS, RDF.type, OWL.Class))
        g.add((spatial_feature_cls, RDF.type, OWL.Class))
        g.add((AREA_FEATURE_CLS, RDFS.subClassOf, spatial_feature_cls))
        g.add((LINE_FEATURE_CLS, RDFS.subClassOf, spatial_feature_cls))
        g.add((POINT_FEATURE_CLS, RDFS.subClassOf, spatial_feature_cls))

        return g

    def write_hotel_data(self, output_dir):

        car_friendly_hotels = []
        for i in range(self.num_pos):
            car_friendly_hotels.append(self.generate_car_friendly_hotel())

        not_car_friendly_hotels = []
        for i in range(self.num_neg):
            not_car_friendly_hotels.append(
                self.generate_not_car_friendly_hotel())

        with open(os.path.join(output_dir, 'load_hotels.sql'), 'w') as sql_file:
            for hotel in car_friendly_hotels + not_car_friendly_hotels:
                sql_file.write(hotel.to_pg_sql())

        with open(os.path.join(output_dir, 'hotels.ttl'), 'wb') as kb_file:
            kb = Graph()

            kb += self.get_hotels_ontology()
            for hotel in car_friendly_hotels + not_car_friendly_hotels:
                kb += hotel.to_rdf()

            kb.serialize(kb_file, 'turtle')

        with open(os.path.join(output_dir, 'pos.txt'), 'w') as pos_file:
            for hotel in car_friendly_hotels:
                pos_file.write(hotel.get_iri() + os.linesep)

        with open(os.path.join(output_dir, 'neg.txt'), 'w') as neg_file:
            for hotel in not_car_friendly_hotels:
                neg_file.write(hotel.get_iri() + os.linesep)

        # print('GEOMETRYCOLLECTION(' + ','.join(
        #     [str(h) for h in car_friendly_hotels + not_car_friendly_hotels]) +
        #     ')')
