import logging

import psycopg2
from rdflib import Graph, URIRef


class PostGISDataLoader(object):
    """
    Given a path to an RDF file objects of this class go through the whole file
    looking for geometry resources, converting them to PostGIS SQL inserts and
    writing them to a PostGIS database.
    An RDF resource is considered a geometry resource if is has a literal
    assigned via an RDF property contained in the provided list
    `geometry_rdf_properties` of known geometry properties.

    TODO: Allow different reference systems
    """
    def __init__(
            self,
            geometry_resource_properties,
            geometry_literal_properties,
            point_feature_classes,
            line_feature_classes,
            area_feature_classes,
            db_name,
            db_host='localhost',
            db_port=5432,
            db_user='postgres',
            db_pw='postgres'):

        self.geometry_resource_properties = \
            [URIRef(uri_str) for uri_str in geometry_resource_properties]
        self.geometry_literal_properties = \
            [URIRef(uri_str) for uri_str in geometry_literal_properties]
        self.point_feature_classes = \
            [URIRef(cls) for cls in point_feature_classes]
        self.line_feature_classes = \
            [URIRef(cls) for cls in line_feature_classes]
        self.area_feature_classes = \
            [URIRef(cls) for cls in area_feature_classes]
        self.db_name = db_name
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pw = db_pw

    _suffix_to_format = {
        'nt': 'ntriples',
        'ttl': 'turtle',
        'rdf': 'xml',
        'xml': 'xml'
    }

    def _guess_format(self, file_path):
        suffix = file_path.split('.')[-1].lower()
        return self._suffix_to_format.get(suffix)

    def _find_and_load_geometry_data(
            self,
            geometry_resource_property: URIRef,
            geometry_literal_property: URIRef,
            g: Graph):

        query_res = g.query(f"""
                SELECT ?feature_cls ?geom_res ?geom_lit
                WHERE {{
                    ?feature_res
                            a ?feature_cls ;
                            <{str(geometry_resource_property)}> ?geom_res .
                    ?geom_res <{str(geometry_literal_property)}> ?geom_lit . 
                }}
                """)

        conn = psycopg2.connect(
            dbname=self.db_name,
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_pw)
        cursor = conn.cursor()

        for feature_cls, geom_res, geom_lit in query_res:
            if feature_cls in self.point_feature_classes:
                table = 'point_feature'
            elif feature_cls in self.line_feature_classes:
                table = 'line_feature'
            elif feature_cls in self.area_feature_classes:
                table = 'area_feature'
            else:
                logging.error(f'Unknown feature class <{str(feature_cls)}>')
                continue

            geom_res_str = str(geom_res)
            wkt_expr = str(geom_lit)
            cursor.execute(f"""
            INSERT INTO {table}
            VALUES ('{geom_res_str}', ST_GeomFromText('{wkt_expr}'))
            """)

        conn.commit()
        cursor.close()
        conn.close()

    def load_geometry_data(self, rdf_file_path):
        guessed_format = self._guess_format(rdf_file_path)
        g = Graph()
        g.load(rdf_file_path, format=guessed_format)

        for geom_res_prop in self.geometry_resource_properties:
            for geom_lit_prop in self.geometry_literal_properties:
                self._find_and_load_geometry_data(geom_res_prop, geom_lit_prop, g)


def init_db(
        db_name,
        db_host='localhost',
        db_port=5432,
        db_user='postgres',
        db_pw='postgres'):
    conn = psycopg2.connect(
        dbname=db_name,
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_pw)
    cursor = conn.cursor()
    cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;')

    cursor.execute("""
    CREATE TABLE area_feature (
        iri character varying(255),
        the_geom public.geometry
    );
    """)

    cursor.execute("""
    CREATE TABLE line_feature (
        iri character varying(255),
        the_geom public.geometry
    );
    """)

    cursor.execute("""
    CREATE TABLE point_feature (
        iri character varying(255),
        the_geom public.geometry
    );
    """)

    conn.commit()
    cursor.close()
    conn.close()
