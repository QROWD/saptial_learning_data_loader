import logging
import math
import os
from random import choice
from random import gauss
from random import randint
from random import random
from random import uniform

from rdflib import Graph, Literal, URIRef, RDF

from dataloader.datasampler import DataSampler

logging.basicConfig(level=logging.INFO)


class DataGenerator(object):
    wkt_dtype = URIRef('http://www.opengis.net/ont/geosparql#wktLiteral')
    geosparql_as_wkt = URIRef('http://www.opengis.net/ont/geosparql#asWKT')
    geosparql_has_geometry = \
        URIRef('http://www.opengis.net/ont/geosparql#hasGeometry')
    ns = 'http://dl-learner.org/spatial#'

    def __init__(self, center_lon, center_lat, output_dir):
        self.center_lon = center_lon
        self.center_lat = center_lat
        self.output_dir = output_dir

        self.min_line_points = 3
        self.max_line_points = 16

        self.min_polygon_points = 3
        self.max_polygon_points = 20

        self.point_type = 'point'
        self.line_string_type = 'line string'
        self.polygon_type = 'polygon'
        self.polygon_types = [
            self.point_type, self.line_string_type, self.polygon_type]

        self.point_table_name = 'point'
        self.line_string_table_name = 'line_string'
        self.polygon_table_name = 'polygon'

        self._neighboring_point_distance = 0.0005
        self._max_polygon_neighboring_distance = 0.001
        self._area_for_10_samples = 0.00002
        self.__tmp_num_samples = 0
        self.__tmp_span = 0

    def _rnd_point(self):
        lon = round(uniform(
            self.center_lon - self.__tmp_span,
            self.center_lon + self.__tmp_span), 4)
        lat = round(uniform(
            self.center_lat - self.__tmp_span,
            self.center_lat + self.__tmp_span), 4)

        return lon, lat

    def _rnd_neighboring_point(self, point):
        lon = round(gauss(point[0], self._neighboring_point_distance), 4)
        lat = round(gauss(point[1], self._neighboring_point_distance), 4)

        return lon, lat

    def _generate_point(self):
        lon, lat = self._rnd_point()
        return f'POINT({lon} {lat})'

    @staticmethod
    def _line_up_points(start_point, other_points):
        points = [start_point]

        for i in range(len(other_points)):
            curr_point = points[i]
            shortest_distance = 99999999
            nearest_point_idx = 999999999
            for tmp_pnt_idx in range(len(other_points)):
                p = other_points[tmp_pnt_idx]
                distance = math.sqrt(
                    ((curr_point[0]-p[0])**2)+((curr_point[1]-p[1])**2))
                if distance < shortest_distance:
                    shortest_distance = distance
                    nearest_point_idx = tmp_pnt_idx

            points.append(other_points.pop(nearest_point_idx))

        return points

    def _generate_line_string(self):
        num_points = randint(self.min_line_points, self.max_line_points)
        start_point = self._rnd_point()
        tmp_points = []

        for i in range(num_points-1):
            if i == 0:
                tmp_points.append(self._rnd_neighboring_point(start_point))
            else:
                tmp_points.append(self._rnd_neighboring_point(tmp_points[i-1]))

        points = self._line_up_points(start_point, tmp_points)

        point_seq = ', '.join(
            map(lambda p: str(p[0]) + ' ' + str(p[1]), points))

        return f'LINESTRING({point_seq})'

    def _generate_polygon(self):
        num_points = randint(self.min_polygon_points, self.max_polygon_points)
        start_point = self._rnd_point()
        tmp_points = [start_point]

        step = 2 * math.pi / num_points
        steps = [step * i for i in range(num_points)]
        stretch = 0.9
        # Multiplier are used to make it less likely that polygons have
        # self-intersection line segments. Using these multipliers will make
        # it more likely that we will get random convex polygons.
        multiplier_lat = [stretch * math.cos(s) for s in steps]
        multiplier_lon = [stretch * math.sin(s) for s in steps]

        for i in range(1, num_points):
            prev_lon, prev_lat = tmp_points[i - 1]

            lon = prev_lon + (random() *
                              self._max_polygon_neighboring_distance *
                              multiplier_lon[i])
            lat = prev_lat + (random() *
                              self._max_polygon_neighboring_distance *
                              multiplier_lat[i])

            tmp_points.append((lon, lat))

        points = tmp_points
        points.append(points[0])

        point_seq = ', '.join(
            map(lambda p: str(p[0]) + ' ' + str(p[1]), points))

        return f'POLYGON(({point_seq}))'

    def _write_kb(self, polygons, file_path):
        g = Graph()

        for wkt_str in polygons:
            wkt_lit = Literal(wkt_str, None, self.wkt_dtype)
            feature_cls = DataSampler._get_feature_cls(wkt_lit)
            hsh = hash(wkt_str)
            feature_res = URIRef(self.ns + f'feature_{hsh}')
            geom_res = URIRef(self.ns + f'geometry_{hsh}')

            g.add((feature_res, RDF.type, feature_cls))
            g.add((feature_res, self.geosparql_has_geometry, geom_res))
            g.add((geom_res, self.geosparql_as_wkt, wkt_lit))

        with open(file_path, 'wb') as out_file:
            g.serialize(out_file, 'turtle')

    def _write_pg_script(self, polygons, file_path):
        with open(file_path, 'w') as out_file:
            for wkt_str in polygons:
                if wkt_str.lower().startswith('point'):
                    table_name = self.point_table_name
                elif wkt_str.lower().startswith('line'):
                    table_name = self.line_string_table_name
                elif wkt_str.lower().startswith('polygon'):
                    table_name = self.polygon_table_name
                else:
                    raise RuntimeError(f'Unknown table for {wkt_str}')

                hsh = hash(wkt_str)
                geom_iri = self.ns + f'geometry_{hsh}'
                sql_str = f"INSERT INTO {table_name} " \
                          f"VALUES ('{geom_iri}', ST_GeomFromText('{wkt_str}');"

                out_file.write(sql_str + os.linesep)

    def generate(self, num_samples):
        self.__tmp_num_samples = num_samples
        self.__tmp_span = \
            math.sqrt(((num_samples / 10) * self._area_for_10_samples))

        polygons = []
        for i in range(num_samples):
            polygon_type = choice(self.polygon_types)

            if not polygon_type == self.polygon_type:
                polygon_type = choice(self.polygon_types)

            if polygon_type == self.point_type:
                p = self._generate_point()
            elif polygon_type == self.line_string_type:
                p = self._generate_line_string()
            else:
                p = self._generate_polygon()

            polygons.append(p)

        kb_file_name = f'kb_{num_samples}.ttl'
        self._write_kb(polygons, os.path.join(self.output_dir, kb_file_name))

        pg_file_name = f'load_{num_samples}.sql'
        self._write_pg_script(
            polygons, os.path.join(self.output_dir, pg_file_name))
        logging.debug('GEOMETRYCOLLECTION(' + ', '.join(polygons) + ')')
