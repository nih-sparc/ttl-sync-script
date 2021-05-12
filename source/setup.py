from setuptools import setup

setup(
    name='ttl_update',
    version='0.2',
    py_modules=[
        'metadata_versions', 'new_metadata','parse_json',
        'base',
        'ttl_update_cli',
        'bf_io','config'],
    install_requires=[
        'Click','Pennsieve','beautifulsoup4',
        'configparser','rdflib',
        'requests','structlog'
    ],
    entry_points='''
        [console_scripts]
        ttl_update=ttl_update_cli:cli
    ''',
)
