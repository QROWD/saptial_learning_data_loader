from setuptools import setup

setup(
    name='spatial_learning_data_loader',
    version='0.0.1',
    packages=[''],
    url='https://github.com/QROWD/spatial_learning_data_loader',
    license='Apache License v2.0',
    author='Patrick Westphal',
    author_email='patrick.westphal@informatik.uni-leipzig.de',
    description='',
    install_requires=[
        'rdflib==4.2.2',
        'psycopg2==2.7.7',
    ],
    scripts=[
        'bin/loaddata',
        'bin/initdb',
        'bin/convertusergpsdata',
        'bin/generatedata'
    ],
)
