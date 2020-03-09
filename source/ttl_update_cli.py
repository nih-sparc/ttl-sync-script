import click
import metadata_versions
import new_metadata
from blackfynn import Blackfynn
import os

# bf = Blackfynn( api_host=os.environ['BLACKFYNN_API_HOST'] )

@click.group()
def cli():
    pass

@click.command()
def get_ttl():
    out = metadata_versions.getLatestTTLVersion()

@click.command()
def ttl2json():
    out = new_metadata.buildJson('full')

cli.add_command(get_ttl)
cli.add_command(ttl2json)