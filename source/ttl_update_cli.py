import click

import bf_io
import metadata_versions
import new_metadata
import os, logging
from config import Configs
from parse_json import update_datasets
from bf_io import clear_dataset



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
@click.option('-v', '--version', default=0, type=int, help="Provide an offset from latest version (e.g. -1)")
def ttl_to_json(version):
    """Get TTL file and convert to JSON.

    This script will download the TTL file and convert to the JSON structure that
    is used to import into the platform. You can provide an offset to get an older 
    version of the TTL file. An offset of of -1 will get the previous TTL file. 
    """

    if version == 0:
        log.info("Getting Latest Version")
        metadata_versions.getLatestTTLVersion()
        new_metadata.buildJson(version)
    elif version < 0:
        log.info("Getting Specific version: {}".format(version))
        out = metadata_versions.getSpecificTTLVersion(version)
        new_metadata.buildJson(version)
    else:
        log.warning('Incorrect argument for version (version > 0)')


@click.command()
@click.argument('env', nargs=1)
@click.argument('id', nargs=-1)
@click.option('-f', '--force_update', default=False, type=bool, help="Forcing to update all models and records (and rebuild hash table for synchronizing future runs)")
@click.option('-fm', '--force_model', default='', help= "force updating records for a single model (specify the model name)" )
@click.option('-r', '--resume', default=False, type=bool, help= "If 'True', then resume synchronizing from previous run. This can be used when the previous run failed to complete."  )
def update(env, id=None, force_update=False, force_model='', resume=False):
    """Synchronize JSON File and Platform.

    This script takes the JSON file that was converted from the TTL file and 
    synchronizes the content with the platform. The script leverages the 
    stored hash-table from the previous run to identify which records should be
    updated on the platform and which records have not been changed. 

    ENV is the name of the environment to synchronize (prod/dev). \n
    ID (optional) can be used to specify the dataset ID that should be synchronized
    """
    if env in ['prod', 'dev']:
        log.info('Starting UPDATE for: {}'.format(env))
        cfg = Configs(env)
        if id:
            out = update_datasets(cfg, id[0], force_update, force_model, resume)
        else:
            out = update_datasets(cfg, 'full', force_update, force_model, resume)
    else:
        log.warning('Incorrect argument (''prod'', ''dev'')')


@click.command()
@click.argument('env', nargs=1)
@click.argument('dataset_id', nargs=1)
def clear_dataset(env, dataset_id=None):
    """Removes all SPARC models from dataset"""

    if env in ['prod', 'dev']:
        log.info('Starting CLEAN_MODEL for: {}'.format(env))
        cfg = Configs(env)
        ds = cfg.bf.get_dataset(dataset_id)
        log.info(ds)
        out = bf_io.clear_dataset(None, ds)

    else:
        log.warning('Incorrect argument (''prod'', ''dev'')')


cli.add_command(clear_dataset)
cli.add_command(ttl_to_json)
cli.add_command(update)
