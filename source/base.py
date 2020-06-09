'Shared helper functions and constants'
import logging
from datetime import datetime as DT
import boto3
from botocore.exceptions import ClientError
import requests
from requests.compat import quote_plus
from boto3.dynamodb.conditions import Key
from time import sleep
import json
import copy

from rdflib import BNode, Graph, URIRef, term


log = logging.getLogger(__name__)

### Settings ###
MODEL_NAMES = ('protocol', 'researcher', 'sample', 'subject', 'summary', 'term', 'award', 'human_subject','animal_subject')
JSON_METADATA_EXPIRED = '/tmp/expired_metadata.json'
JSON_METADATA_FULL = '/tmp/full_metadata.json'
JSON_METADATA_NEW = '/tmp/new_metadata.json'
TTL_FILE_OLD = '/tmp/curation-export-old.ttl'
TTL_FILE_NEW = '/tmp/curation-export-new.ttl'
TTL_FILE_DIFF = '/tmp/curation-export-diff.ttl'
SPARC_DATASET_ID = 'N:dataset:bed6add3-09c0-4834-b129-c3b406240f3d'

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
    'isAboutParticipant',
    'hasContactPerson',
    'hasResponsiblePrincipalInvestigator',
    'raw/wasExtractedFromAnatomicalRegion',
    'description']

### Helper functions ###

def has_bf_access(ds):
    """Check that curation team has manager access

    Parameters
    ----------
    ds: BFDataset
        Dataset that is checked
    """

    teams = ds.team_collaborators()
    for team in teams:
        if team.name == 'SPARC Data Curation Team':
            if team.role == 'manager':
                return True
    
    return False

def parse_unit_value(node, name, model_unit = 'None', is_num=True):
    """Method that returns a value that is associated with a unit

    Method does the following:
    1) Checks for two types of unitValue representations
        a) {value: [value], unit: unit}
        b) "value unit"
    2) Checks if the unit matches the unit of the model it will be added to.
    3) Parses the value to a float and return.

    Parameters
    ----------
    node: {}
        Node with representation of record entity
    name: str
        Name of the property 
    model_unit: str, optional
        Unit of the property as defined in Model Schema

    """
    
    value = None
    unit = None

    # Check if node name exists
    if not name in node:
        log.warning('No value for {}'.format(name))
        return None

    # Check is coded as unit or string
    if isinstance(node[name], dict):
        value = node[name]['value']
        unit = node[name]['unit']
    else:
        # assume string is "value unit"
        v = node[name].split()
        value = v[0]
        if len(v)>1:
            unit = v[1]

    # Validate that unit matches Model Unit.
    if unit != model_unit:
        log.warning('Unit mismatch between record and model {} - {}'.format(unit, model_unit))

    if is_num:
        try:
            value = float(value)
        except ValueError:
            log.warning("Cannot parse float value even though dataset has float values for Measurement")
            value = None
    else:
        value = '{} {}'.format(value, unit)
    # try converting to float
    # try:
    #     value = float(value)
    # except:
    #     value = value

    # Return value
    return value

def get_as_list(subNode, key):
    value = None
    if key in subNode:
        if isinstance(subNode.get(key), list):
            value = subNode.get(key)
        else:
            value = [subNode.get(key)]
    return value

def iri_lookup(g, iri, iriCache=None):
    'Retrieve data about a SPARC term'
    skipIri = (
        'http://uri.interlex.org',
        'https://api.blackfynn.io',
        'https://app.blackfynn.io',
        'https://orcid.org',
        'https://doi.org',
        'https://ror.org',
        'http://dx.doi.org/',
        'https://scicrunch.org/resolver/',
        'https://www.protocols.io/')
    
    apiKey = '8a72SmzPaTtrail8ySNWgtSTuJgMyAtZ'

    if iriCache is None:
        iriCache = {}

    # Check if defined in TTL file
    if (URIRef(iri), None, URIRef('http://www.w3.org/2002/07/owl#NamedIndividual')) in g:
        if (URIRef(iri), URIRef('http://www.w3.org/2000/01/rdf-schema#label'), None) in g:

            # URIRef is defined elsewhere in the TTL File

            # URI is an RRID --> Should be defined in TTL
            if iri.startswith('https://scicrunch.org/resolver/RRID:'):
                out = g.objects(subject= URIRef(iri), predicate=URIRef('http://www.w3.org/2000/01/rdf-schema#label'))
                for v in out:
                    result_dict = {'iri': iri,
                        'labels': [v],
                        'curie': strip_iri(iri)}
                    iriCache[iri] = result_dict
                return result_dict

            # URI is an UBERON term --> is defined, but get more details from scicrunch
            elif iri.startswith('http://purl.obolibrary.org/obo/UBERON_'):
                url = 'https://scicrunch.org/api/1/sparc-scigraph/vocabulary/id/{}?key={}'.format(
                    quote_plus(iri), apiKey)
                r = requests.get(url)
                if r.status_code == 200:
                    log.debug('SciCrunch lookup successful: %s', iri)
                    iriCache[iri] = r.json()
                    return r.json()
                else:
                    log.error('SciCrunch HTTP Error: %d %s iri= %s', r.status_code, r.reason, iri)


    if any(iri.startswith(s) for s in skipIri):
        return strip_iri(iri.strip())
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
    # url = 'https://scicrunch.org/api/1/sparc-scigraph/vocabulary/id/{}?key={}'.format(
    #     quote_plus(iri), apiKey)
    # r = requests.get(url)
    # if r.status_code == 200:
    #     log.debug('SciCrunch lookup successful: %s', iri)
    #     iriCache[iri] = r.json()
    #     return r.json()
    # log.error('SciCrunch HTTP Error: %d %s iri= %s', r.status_code, r.reason, iri)

def get_first(node, name, default=None):
    try:
        return node[name][0]
    except (KeyError, IndexError):
        return default

### Helper function to check li
def contains(list, filter):
    for x in list:
        if filter(x):
            return x
    return False

def is_number(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

### Parsing JSON data:
def get_json():
    '''Load JSON files containing expired and new metadata'''
    with open(JSON_METADATA_FULL, 'r') as f:
        log.info("Loaded '{}'".format(JSON_METADATA_FULL))
        data = json.load(f)
    return data

def get_resume_list(file_name):
    '''Load JSON files containing resume info'''
    with open(file_name, 'r') as f:
        log.info("Loaded '{}'".format(file_name))
        data = json.load(f)
        print(data)
    return data

def get_bf_model(ds, name):
    """Return the model with name in dataset

        This method return the Blackfynn Model with a particular
        name for a particular dataset. The method provides a cache
        to prevent an API call when the model has previously been 
        loaded
    """

    if not hasattr(get_bf_model, "models"):
        log.debug('SETUP MODEL CACHE')
        model = ds.get_model(name)
        get_bf_model.models = {name: model}
        get_bf_model.model_ds = ds.id
        return model
    elif get_bf_model.model_ds != ds.id:
        log.debug('SWITCHING DS')
        model = ds.get_model(name)
        get_bf_model.models = {name: model}
        get_bf_model.model_ds = ds.id
        return model
    else:
        if name in get_bf_model.models:
            log.debug('RETURN MODEL FROM CACHE')
            return get_bf_model.models[name]
        else:
            log.debug('ADDING MODEL TO CACHE')
            try:
                # Get model from platform and add to cache
                model = ds.get_model(name)
                get_bf_model.models.update({name: model})
                return model
            except:
                # Model does not exist on the platform
                return None

def get_record_by_id(json_id, model, record_cache):
    """Get Blackfynn Record by its JSON ID

        The JSON_ID should be "RecordName". Record Cache is 
        cleared out between imports of datasets so only
        represents a single dataset

        The record_cache should map JSON_ID to Blackfynn_ID
    """
    
    # Because we expect that this exist at this point.
    if not json_id in record_cache[model.type]:
        raise(Exception("JSON-ID: {}".format(json_id)))

    # Get the Blackfynn ID, or the Blackfynn Record from cache
    bf_obj = record_cache[model.type][json_id]

    # Fetch the Record and return
    if isinstance(bf_obj, str):
        return model.get(bf_obj)
    else:
        return bf_obj

def strip_iri(iri):
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
        'http://purl.org/dc/elements/1.1/',
        'https://scicrunch.org/resolver/RRID:'

        )

    for s in strips:
        if s in iri:
            return iri.replace(s, '')
    return iri
