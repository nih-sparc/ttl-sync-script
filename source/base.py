'Shared helper functions and constants'
import logging

import boto3
from botocore.exceptions import ClientError
#from moto import mock_ssm
import requests
from requests.compat import quote_plus

from config import Configs

cfg = Configs()
log = logging.getLogger(__name__)
log.setLevel('INFO')

### Settings ###
JSON_METADATA_EXPIRED = '/tmp/expired_metadata.json'
JSON_METADATA_FULL = '/tmp/full_metadata.json'
JSON_METADATA_NEW = '/tmp/new_metadata.json'
TTL_FILE_OLD = '/tmp/curation-export-old.ttl'
TTL_FILE_NEW = '/tmp/curation-export-new.ttl'
INSTRUCTION_FILE = '/tmp/instructions_list.txt'
SPARC_DATASET_ID = 'N:dataset:bed6add3-09c0-4834-b129-c3b406240f3d'

# List of dataset Ids to skip instead of updating
SKIP_LIST = []

# List of properties which have multiple values:
arrayProps = [
    'http://purl.obolibrary.org/obo/IAO_0000136',
    'hasExperimentalModality',
    'hasAffiliation',
    'protocols',
    'involvesAnatomicalRegion',
    'spatialLocationOfModulator',
    'hasAssignedGroup',
    'IsDescribedBy',
    'hasRole',
    'protocolExecutionDate',
    'localExecutionNumber',
    'providerNote',
    'TODO',
    'hasDigitalArtifactThatIsAboutItWithHash',
    'hasDigitalArtifactThatIsAboutIt',
    'protocolEmploysTechnique',
    'isAboutParticipant']

### AWS ###
class SSMClient():
    'Wrapper class for getting/setting SSM properties'
    #@mock_ssm
    def __init__(self):
        self.client = boto3.client('ssm', region_name=cfg.aws_region)

    #@mock_ssm
    def get(self, name, default=None):
        try:
            resp = self.client.get_parameter(Name=cfg.ssm_path + name)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                return default
            raise
        else:
            return resp['Parameter']['Value']

    #@mock_ssm
    def set(self, name, value, Type='String'):
        self.client.put_parameter(
            Name=cfg.ssm_path + name,
            Value=value,
            Type=Type,
            Overwrite=True)

### Helper functions ###
def iriLookup(iri, iriCache=None):
    'Retrieve data about a SPARC term'
    skipIri = (
        'http://uri.interlex.org',
        'https://api.blackfynn.io',
        'https://app.blackfynn.io',
        'https://orcid.org',
        'https://doi.org',
        'https://ror.org',
        'http://dx.doi.org/')

    if iriCache is None:
        iriCache = {}
    if any(iri.startswith(s) for s in skipIri):
        return stripIri(iri.strip())
    if iri in iriCache:
        log.debug('Returning cached IRI: %s', iri)
        return iriCache[iri]
    # if 'ror.org' in iri:
    #     # Use ROR API
    #     url = 'https://api.ror.org/organizations/{}'.format(quote_plus(iri))
    #     r = requests.get(url)
    #     if r.status_code == 200:
    #         name = r.json()['name']
    #         iriCache[iri] = name
    #         return name
    #     else:
    #         raise Exception('iriLookup HTTP Error: %d %s\n url= %s' % (r.status_code, r.reason, url))
    
    # use SciCrunch API
    apiKey = '8a72SmzPaTtrail8ySNWgtSTuJgMyAtZ'
    url = 'https://scicrunch.org/api/1/sparc-scigraph/vocabulary/id/{}?key={}'.format(
        quote_plus(iri), apiKey)
    r = requests.get(url)
    if r.status_code == 200:
        log.debug('SciCrunch lookup successful: %s', iri)
        iriCache[iri] = r.json()
        return r.json()
    log.error('SciCrunch HTTP Error: %d %s iri= %s', r.status_code, r.reason, iri)

def stripIri(iri):
    'Remove the base URL of an IRI'
    strips = (
        'http://uri.interlex.org/tgbugs/uris/readable/technique/',
        'http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/',
        'http://uri.interlex.org/tgbugs/uris/readable/sparc/',
        'http://uri.interlex.org/temp/uris/awards/',
        'http://uri.interlex.org/temp/uris/',
        'https://api.blackfynn.io/users/',
        'https://api.blackfynn.io/datasets/',

        'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'http://www.w3.org/2000/01/rdf-schema#',
        'http://purl.org/dc/elements/1.1/'
        )

    for s in strips:
        if s in iri:
            return iri.replace(s, '')
    return iri
