from setuptools import setup

setup(
    name='ttl_update',
    version='0.1',
    py_modules=[
        'metadata_versions', 'new_metadata','parse_json',
        'base',
        'ttl_update_cli',
        'bf_io','config'],
    install_requires=[
        'Click','Blackfynn','beautifulsoup4',
        'configparser','rdflib',
        'requests','structlog','pyhash'
    ],
    entry_points='''
        [console_scripts]
        ttl_update=ttl_update_cli:cli
    ''',
)