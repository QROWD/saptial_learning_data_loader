import os
import random

from rdflib import Graph, URIRef, RDF

import logging
logging.basicConfig(level=logging.DEBUG)

GEOVOCAB_GEOMETRY = URIRef('http://geovocab.org/geometry#geometry')
GEOSPARQL_AS_WKT = URIRef('http://www.opengis.net/ont/geosparql#asWKT')

POINT_FEATURE_CLS = URIRef('http://dl-learner.org/spatial#PointFeature')
LINE_FEATURE_CLS = URIRef('http://dl-learner.org/spatial#LineFeature')
AREA_FEATURE_CLS = URIRef('http://dl-learner.org/spatial#AreaFeature')


class DataSampler(object):
    def __init__(self, data_dir, owl_output_file_path, pg_output_file_path):
        self.data_dir = data_dir
        self.nt_files = [f for f in os.listdir(data_dir) if f.endswith('.nt')]

        self.owl_output_file_path = owl_output_file_path
        self.pg_output_file_path = pg_output_file_path

        self.point_table_name = 'point'
        self.line_str_table_name = 'line_string'
        self.polygon_table_name = 'polygon'

    @staticmethod
    def _get_feature_cls(wkt_lit):
        wkt_lit_str = str(wkt_lit)
        if wkt_lit_str.startswith('POINT'):
            return POINT_FEATURE_CLS
        elif wkt_lit_str.startswith('LINE'):
            return LINE_FEATURE_CLS
        elif wkt_lit_str.startswith('POLY'):
            return AREA_FEATURE_CLS
        else:
            raise RuntimeError(f'Unknown type of {wkt_lit_str}')

    def sample(self):
        triple_counts = self._get_triple_counts()
        total_triple_count = sum(map(lambda c: c[1], triple_counts.items()))

        for num_samples in [
                10, 50,
                100, 500,
                1000, 5000,
                10000, 50000,
                1000000, 5000000,
                10000000, 50000000]:

            sample_ratio = num_samples / total_triple_count

            result_graph = Graph()
            result_sql_content = ''

            samples_extracted = 0
            for filename, count in triple_counts.items():
                num_triples_to_sample = round(count * sample_ratio)
                if num_triples_to_sample == 0:
                    logging.warn(
                        f'File {filename} skipped due to too few triples')
                    continue

                logging.info(f'Loading {filename} for sampling '
                             f'{num_triples_to_sample} triples')
                g = Graph()
                g.load(os.path.join(self.data_dir, filename), format='ntriples')

                query_result = g.query(f"""
                    SELECT ?feature ?geom ?wkt_lit ?rand
                    WHERE {{
                        ?feature <http://geovocab.org/geometry#geometry> ?geom .
                        ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt_lit .
                        BIND( RAND() AS ?rand)
                    }} ORDER BY ?rand LIMIT {num_triples_to_sample}
                    """)

                for feature, geom, wkt_lit, _ in query_result:
                    feature_cls = self._get_feature_cls(wkt_lit)
                    result_graph.add((feature, RDF.type, feature_cls))
                    result_graph.add((feature, GEOVOCAB_GEOMETRY, geom))
                    result_graph.add((geom, GEOSPARQL_AS_WKT, wkt_lit))

                    table_name = self._get_table_name(wkt_lit)

                    result_sql_content += \
                        f"INSERT INTO {table_name} " \
                        f"VALUES (" \
                            f"'{geom}', ST_GeomFromText('{str(wkt_lit)}')); \n"

                samples_extracted += num_triples_to_sample
                del g

                while samples_extracted < num_samples:
                    filename = random.choice(triple_counts)

                    logging.info(f'Loading randomly chosen file {filename} '
                                 f'to sample 1 triple')
                    g = Graph()
                    g.load(os.path.join(self.data_dir, filename),
                           format='ntriples')

                    query_result = g.query(f"""
                        SELECT ?feature ?geom ?wkt_lit ?rand
                        WHERE {{
                            ?feature <http://geovocab.org/geometry#geometry> ?geom .
                            ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt_lit .
                            BIND( RAND() AS ?rand)
                        }} ORDER BY ?rand LIMIT 1
                        """)

                    for feature, geom, wkt_lit, _ in query_result:
                        result_graph.add((feature, GEOVOCAB_GEOMETRY, geom))
                        result_graph.add((geom, GEOSPARQL_AS_WKT, wkt_lit))

                        table_name = self._get_table_name(wkt_lit)

                        result_sql_content += \
                            f"INSERT INTO {table_name} " \
                            f"VALUES (" \
                                f"'{geom}', " \
                                f"ST_GeomFromText('{str(wkt_lit)}')); \n"

                    samples_extracted += 1

            with open(self.pg_output_file_path + f'_{num_samples}', 'w') as pg_out:
                pg_out.write(result_sql_content)

            with open(self.owl_output_file_path + f'_{num_samples}', 'wb') as owl_out:
                result_graph.serialize(owl_out, format='ntriples')

    def _get_table_name(self, wkt_lit):
        val = str(wkt_lit)

        if val.startswith('POINT'):
            return self.point_table_name
        elif val.startswith('LINE'):
            return self.line_str_table_name
        elif val.startswith('POLYGON'):
            return self.point_table_name
        else:
            raise Exception(f'Unhandled polygon type: {val}')

    def _get_triple_counts(self):
        counts = {}
        for nt_file in self.nt_files:
            logging.info(f'Getting count for {nt_file}')
            g = Graph()
            g.load(os.path.join(self.data_dir, nt_file), format='ntriples')

            query_res = g.query("""
                    SELECT (count(?s) AS ?count)
                    WHERE {
                        ?s <http://www.opengis.net/ont/geosparql#asWKT> ?o .
                    }""")

            query_res = [r for r in query_res]
            assert len(query_res) == 1

            count = query_res[0][0].value
            counts[nt_file] = count

            del g

        return counts
