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

log = logging.getLogger(__name__)

### Settings ###
MODEL_NAMES = ('protocol', 'researcher', 'sample', 'subject', 'summary', 'term', 'award', 'human_subject','animal_subject')
JSON_METADATA_EXPIRED = '/tmp/expired_metadata.json'
JSON_METADATA_FULL = '/tmp/full_metadata.json'
JSON_METADATA_NEW = '/tmp/new_metadata.json'
TTL_FILE_OLD = '/tmp/curation-export-old.ttl'
TTL_FILE_NEW = '/tmp/curation-export-new.ttl'
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

### AWS ###
class SSMClient():
    'Wrapper class for getting/setting SSM properties'
    #@mock_ssm
    def __init__(self, aws_region, env, ssm_path, endpoint):
        log.info('Getting SSM client for: {} - {}'.format(endpoint, env))
        log.info('---')
        self.region = aws_region
        self.ssm_path = ssm_path
        self.client = boto3.client('ssm', region_name=self.region, endpoint_url = endpoint)

    #@mock_ssm
    def get(self, name, default=None):
        try:
            resp = self.client.get_parameter(Name=self.ssm_path + name)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                return default
            raise
        else:
            return resp['Parameter']['Value']

    #@mock_ssm
    def set(self, name, value, Type='String'):
        self.client.put_parameter(
            Name=self.ssm_path + name,
            Value=value,
            Type=Type,
            Overwrite=True)

### DynamoDB ###
class DynamoDBClient():
    ### Database I/O
    environment_name = None

    def __init__(self, aws_region, env, particion_key, endpoint, table_id, table_sort_key ):
        log.info('Getting DynamoDB client for: {} - {}'.format(endpoint, env))
        log.info('---')
        self.aws_region = aws_region
        self.environment_name = env
        self.table_partition_key = particion_key
        self.endpoint_url = endpoint
        self.table_id = table_id
        self.table_sort_key = table_sort_key
        if self.environment_name is "prod":
            self.client = boto3.resource('dynamodb', region_name = self.aws_region)
        else:
            self.client = boto3.resource('dynamodb', endpoint_url = endpoint)

    #@mock_dynamodb2
    def getTable(self):

        table = self.client.Table(self.table_id)
        
        try:
            is_table_existing = table.table_status in ("CREATING", "UPDATING", "DELETING", "ACTIVE")
        except ClientError:
            is_table_existing = False
            print("Table {} doesn't exist.".format(table.name))

        if is_table_existing:
            return table
        else:   
            table = self.client.create_table(
                TableName=self.table_id,
                KeySchema=[
                    {'AttributeName': self.table_partition_key, 'KeyType': "HASH"},
                    {'AttributeName': self.table_sort_key, 'KeyType': "RANGE"}
                ],
                AttributeDefinitions=[
                    {'AttributeName': self.table_partition_key, 'AttributeType': 'S'},
                    {'AttributeName': self.table_sort_key, 'AttributeType': 'S'}
                
                ],
                BillingMode="PROVISIONED",
                ProvisionedThroughput={
                    'ReadCapacityUnits': 123,
                    'WriteCapacityUnits': 123
                }
            )
            table.wait_until_exists()
            return table

    #@mock_dynamodb2
    def buildCache(self, dsId):
        """Get records cache from the database,
        
        return a dictionary of {model name: {identifier: record ID}}
        
        """
        
        table = self.getTable()
        log.info('GETTING TABLE FROM DYNAMODB: {}'.format(table.table_status))

        res = table.query(KeyConditionExpression=Key(self.table_partition_key).eq(dsId))
        cache = {m: {} for m in MODEL_NAMES}
        for item in res['Items']:
            cache[item['model']][item['identifier']] = item['recordId']

        log.debug('Retrieved {} database records for {}'.format(res['Count'], dsId))
        return cache

    #@mock_dynamodb2
    def writeCache(self, dsId, recordCache):
        """ Write cache to database

        This method will remove all entries in database that currently exist
        for the provided dsID and replace with those in the recordCache.

        For Synchronization, it is important to make sure the recordCache is populated correctly.
        
        """

        log.info('Writing Cache to DYNAMODB: {} '.format(self.environment_name))

        newEntries = [{
            'datasetId': dsId,
            'model': model,
            'identifier': k,
            'id': v.id}
            for model, records in recordCache.items() for k, v in records.items()]

        log.info('Got {} entries.'.format(len(newEntries)))

        table = self.getTable()
        log.info('GETTING TABLE FROM DYNAMODB: {}'.format(table.table_status))

        # Delete old items for particular dataset from table
        oldItems = table.query(KeyConditionExpression=Key(self.table_partition_key).eq(dsId))
        with table.batch_writer() as batch:
            for item in oldItems['Items']:
                try:
                    batch.delete_item(Key={self.table_partition_key: dsId, self.table_sort_key: item['recordId']})
                except KeyError as e:
                    log.error('Key Error File "/var/task/parse_json.py", line 212, in writeCache')
                except Exception as e:
                    print("an exception occured")
                    print(e)
                    raise(e)

        # Write new entries
        with table.batch_writer() as batch:
            for e in newEntries:
                try:
                    new_item = {
                        self.table_partition_key: e['datasetId'],
                        self.table_sort_key: e['id'],
                        'model': e['model'],
                        'identifier': e['identifier']
                    }
                    batch.put_item(Item=new_item)
                except KeyError as k:
                    log.error('Key Error File "/var/task/parse_json.py", line 221, in writeCache')
                    print(k)
                except Exception as e:
                    print("an exception occured")
                    print(e)
                    raise(e)

        if newEntries:
            log.debug('Inserted {} records'.format(len(newEntries)))
        else:
            log.info('Cleared all database entries for {}'.format(dsId))


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

def parse_unit_value(node, name, model_unit = 'None'):
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
        log.info('No value for {}'.format(name))
        return None

    # Check is coded as unit or string
    if isinstance(node[name], dict):
        value = node[name]['value'][0]
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

    # try converting to float
    try:
        value = float(value)
    except:
        log.warning('Cannot cast as Numeric: {} - {}'.format(name,value))
        value = None

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

def iri_lookup(iri, iriCache=None):
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
    apiKey = '8a72SmzPaTtrail8ySNWgtSTuJgMyAtZ'
    url = 'https://scicrunch.org/api/1/sparc-scigraph/vocabulary/id/{}?key={}'.format(
        quote_plus(iri), apiKey)
    r = requests.get(url)
    if r.status_code == 200:
        log.debug('SciCrunch lookup successful: %s', iri)
        iriCache[iri] = r.json()
        return r.json()
    log.error('SciCrunch HTTP Error: %d %s iri= %s', r.status_code, r.reason, iri)

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

### Parsing JSON data:
def get_json(_type):
    '''Load JSON files containing expired and new metadata'''
    if _type == 'diff':
        with open(JSON_METADATA_EXPIRED, 'r') as f1:
            expired = json.load(f1)
            log.info("Loaded '{}'".format(JSON_METADATA_EXPIRED))
        with open(JSON_METADATA_NEW, 'r') as f2:
            new = json.load(f2)
            log.info("Loaded '{}'".format(JSON_METADATA_NEW))
        return expired, new
    if _type == 'full':
        with open(JSON_METADATA_FULL, 'r') as f:
            log.info("Loaded '{}'".format(JSON_METADATA_FULL))
            data = json.load(f)
        return data
    raise Exception("Must use 'diff' or 'full' option")

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
        'http://purl.org/dc/elements/1.1/'

        )

    for s in strips:
        if s in iri:
            return iri.replace(s, '')
    return iri
