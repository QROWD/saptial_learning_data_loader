import logging
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from statistics import stdev, mean

from rdflib import Graph, URIRef, OWL, RDF, RDFS, XSD, Literal

EARTH_RADIUS = 6367.4445  # approximately...
DEFAULT_ONT_PREFIX = 'http://dl-learner.org/ont/spatial'
MOVE_CLS = URIRef('http://dl-learner.org/ont/spatial#Move')
HAS_SPEED_AVG = URIRef(DEFAULT_ONT_PREFIX + 'has_speed_avg')
HAS_SPEED_STDEV = URIRef(DEFAULT_ONT_PREFIX + 'has_speed_stdev')
GEOMETRY_CLS = URIRef('http://www.opengis.net/ont/geosparql#Geometry')
GEOM_DATATYPE_PROPERTY = URIRef('http://www.opengis.net/ont/geosparql#asWKT')

DEFAULT_RES_PREFIX = 'http://dl-learner.org/res/spatial'


# https://en.wikipedia.org/wiki/Haversine_formula:
# 'the "Earth radius" R varies from 6356.752 km at the poles to 6378.137 km at
# the equator'
def distance(point1, point2):
    lat1, lon1 = point1
    lat2, lon2 = point2
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    lon_delta = lon2 - lon1
    lat_delta = lat2 - lat1

    a = sin(lat_delta / 2)**2 + cos(lat1) * cos(lat2) * sin(lon_delta / 2)**2
    c = 2 * asin(sqrt(a))

    distance_in_km = EARTH_RADIUS * c

    return distance_in_km


def init_ontology():
    g = Graph()
    g.add((HAS_SPEED_AVG, RDF.type, OWL.DatatypeProperty))
    g.add((HAS_SPEED_AVG, RDFS.domain, MOVE_CLS))
    g.add((HAS_SPEED_AVG, RDFS.range, XSD.double))

    g.add((HAS_SPEED_STDEV, RDF.type, OWL.DatatypeProperty))
    g.add((HAS_SPEED_STDEV, RDFS.domain, MOVE_CLS))
    g.add((HAS_SPEED_STDEV, RDFS.range, XSD.double))

    return g


def convert_user_data(input_file_path):
    with open(input_file_path) as input_file:
        all_points = []
        prev_point = None
        prev_timestamp = None
        speeds = []

        g = Graph()
        g += init_ontology()

        for line in input_file:
            # get CSV fields with stripped off double quotes
            user_id, timestamp_str, lon_str, lat_str, label = \
                map(lambda s: s[1:-1], line.strip().split(','))

            curr_point = (float(lat_str), float(lon_str))
            curr_timestamp = datetime.fromisoformat(timestamp_str)

            if prev_point is not None:
                dist_in_km = distance(prev_point, curr_point)
                time_delta_in_secs = \
                    (curr_timestamp - prev_timestamp).total_seconds()
                speed = dist_in_km / (time_delta_in_secs / 60. / 60.)
                speeds.append(speed)
            else:
                from_timestamp = curr_timestamp
            # from... and to... just needed for file naming
            to_timestamp = curr_timestamp

            all_points.append(curr_point)

            prev_point = curr_point
            prev_timestamp = curr_timestamp

        if len(all_points) < 2:
            return g

        wkt_line_string = \
            'LINESTRING(' + \
            ', '.join([f'{lon} {lat}' for lon, lat in all_points]) + ')'

        move_id = f'move_{user_id}_' \
            f'{from_timestamp.isoformat().replace(":", "-")}_-_' \
            f'{to_timestamp.isoformat().replace(":", "-")}'

        move_feature_iri = URIRef(DEFAULT_RES_PREFIX + move_id)
        g.add((move_feature_iri, RDF.type, MOVE_CLS))

        if len(speeds) >= 2:
            avg_speed = mean(speeds)
            speed_stdev = stdev(speeds)

            g.add((move_feature_iri, HAS_SPEED_AVG, Literal(avg_speed, None, XSD.double)))
            g.add((move_feature_iri, HAS_SPEED_STDEV, Literal(speed_stdev, None, XSD.double)))
        else:
            logging.warning(
                f'Too few GPS points for {move_feature_iri} to compute stats')

        move_geom_iri = URIRef(DEFAULT_RES_PREFIX + move_id + '_geom')
        g.add((move_geom_iri, RDF.type, GEOMETRY_CLS))
        g.add((
            move_geom_iri,
            GEOM_DATATYPE_PROPERTY,
            Literal(
                wkt_line_string,
                None,
                URIRef('http://www.opengis.net/ont/geosparql#wktLiteral'))))

        outfile_name = label + '_' + move_id + '.ttl'
        g.serialize(outfile_name, format='turtle')

