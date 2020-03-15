import click
import metadata_versions
import new_metadata
import os, logging
from config import Configs
from parse_json import update_datasets


root_log = logging.getLogger()
root_log.setLevel(logging.INFO)

# Create a file with all warning+ log entries
fh = logging.FileHandler('/tmp/curation_val.log')
fh.setLevel(logging.WARNING)
root_log.addHandler(fh)


log = logging.getLogger(__name__)

@click.group()
def cli():
    pass

@click.command()
def get_ttl():
    out = metadata_versions.getLatestTTLVersion()

@click.command()
@click.argument('method', nargs=1)
def to_json(method):
    if method in ['full','diff']:
        out = new_metadata.buildJson(method)
    else:
        log.warning('Incorrect argument (''full'', ''diff'')')

@click.command()
@click.argument('env', nargs=1)
@click.argument('id', nargs=1)
def update(env, id=None):
    if env in ['prod', 'dev']:
        log.info('Starting UPDATE for: {}'.format(env))
        cfg = Configs(env)
        if id:
            out = update_datasets(cfg, id)
        else:
            out = update_datasets(cfg, 'full')
    else:
        log.warning('Incorrect argument (''prod'', ''dev'')')


cli.add_command(get_ttl)
cli.add_command(to_json)
cli.add_command(update)