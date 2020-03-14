'Shared helper functions and constants'
import logging
from datetime import datetime as DT
import boto3
from botocore.exceptions import ClientError
import requests
from requests.compat import quote_plus
from boto3.dynamodb.conditions import Key
from time import sleep

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
    'raw/wasExtractedFromAnatomicalRegion']

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
        if self.environment_name is not "dev":
            return self.client.Table(self.table_id)
        return self.client.create_table(
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

    #@mock_dynamodb2
    def buildCache(self, dsId):
        '''
        Get records cache from the database,
        return a dictionary of {model name: {identifier: record ID}}
        '''
        table = self.getTable()
        table.wait_until_exists()
        log.info('GETTING TABLE FROM DYNAMODB: {}'.format(table.table_status))

        res = table.query(KeyConditionExpression=Key(self.table_partition_key).eq(dsId))
        cache = {m: {} for m in MODEL_NAMES}
        for item in res['Items']:
            cache[item['model']][item['identifier']] = item['recordId']
        log.debug('Retrieved {} database records for {}'.format(res['Count'], dsId))
        return cache

    #@mock_dynamodb2
    def writeCache(self, dsId, recordCache):
        'Write mappings in recordCache to the db'
        newEntries = [{
            'datasetId': dsId,
            'model': model,
            'identifier': k,
            'id': v}
            for model, records in recordCache.items() for k, v in records.items()]

        table = self.getTable()
        table.wait_until_exists()
        log.info('GETTING TABLE FROM DYNAMODB: {}'.format(table.table_status))

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
                    sys.exit()

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
                    sys.exit()

        if newEntries:
            log.debug('Inserted {} records'.format(len(newEntries)))
        else:
            log.info('Cleared all database entries for {}'.format(dsId))


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
