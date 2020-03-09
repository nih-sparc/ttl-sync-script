'''
> parse_json.py [dataset ids]

Reads JSON files and updates the specified datasets on Blackfynn
If no dataset Ids are given, updates all datasets.
'''
#%% [markdown]
# ### Imports

#%%
from datetime import datetime as DT
import json
import logging
import re
import sys
import os
import requests

from blackfynn import Blackfynn, ModelProperty, LinkedModelProperty
from blackfynn.base import UnauthorizedException
from blackfynn.models import ModelPropertyEnumType, BaseCollection
import boto3
from boto3.dynamodb.conditions import Key
#from moto import mock_dynamodb2
from requests.exceptions import HTTPError
from time import time

from base import (
    JSON_METADATA_EXPIRED,
    JSON_METADATA_FULL,
    JSON_METADATA_NEW,
    SKIP_LIST,
    INSTRUCTION_FILE,
    SPARC_DATASET_ID,
    SSMClient
)
from config import Configs
from pprint import pprint

MODEL_NAMES = ('protocol', 'researcher', 'sample', 'subject', 'summary', 'term', 'award')
logging.basicConfig(format="%(asctime);s%(filename)s:%(lineno)d:\t%(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

cfg = Configs()
ssm = SSMClient()
#%% [markdown]
### Set these variables before running
#%%
# (optional) List of IDs of datasets to update:
DATASETS_LIST = []

# (optional) Set a test dataset to update instead of the real one.
# Otherwise, set to None
#dsTest = Blackfynn().get('My test dataset')
dsTest = None

#%% [markdown]
### Helper functions
#%%
### Blackfynn platform I/O:
class BlackfynnException(Exception):
    'Represents Exceptions raised by the API'
    pass

def bfClient():
    return Blackfynn(api_token=cfg.blackfynn_api_token, api_secret=cfg.blackfynn_api_secret, host=cfg.blackfynn_host)

def authorized(dsId):
    '''check if user is authorized as a manager'''
    api = bf._api.datasets
    try:
        role = api._get(api._uri('/{dsId}/role', dsId=dsId)).get('role')
    except UnauthorizedException:
        return False
    except:
        return False
    return role == 'manager'

def getDataset(dsId):
    return bf.get_dataset(dsId) if dsTest is None else dsTest

def clearDataset(dataset):
    '''
    DANGER! Deletes all records of type:
    - protocol
    - researcher
    - sample
    - subject
    - summary
    - term
    '''
    try:
        models = dataset.models().values()
        for m in models:
            if m.type not in MODEL_NAMES or m.count == 0:
                continue
            recs = m.get_all(limit=m.count)
            m.delete_records(*recs)
    except:
        log.info("Error clearing dataset '{}'".format(dataset.name))
    log.info("Cleared dataset '{}'".format(dataset.name))

def getModel(ds, name, displayName, schema=None, linked=None):
    '''create a model if it doesn't exist,
    or retrieve it and update its schema properties'''
    if schema is None:
        schema = []
    if linked is None:
        linked = []
    try:
        model = ds.get_model(name)
        try:
            for s in schema:
                s.id = model.schema[s.name].id
            newLinks = [l for l in linked if l.name not in model.linked]
            model.schema = {s.name: s for s in schema}
            model.update()
            if newLinks:
                try:
                    model.add_linked_properties(newLinks)
                except Exception as e:
                    log.info("Error adding linked properties '{}' to dataset '{}': {}".format(newLinks, ds.name, e))
        except Exception as e:
            log.info("Error updating model '{}' with schema '{}' to dataset '{}': {}".format(model, schema, ds.name, e))
    except HTTPError:
        #log.info("model '{}' not found in dataset '{}': trying to create it".format(name, ds.name))
        try:
            model = ds.create_model(name, displayName, schema=schema)
            if linked:
                model.add_linked_properties(linked)
        except Exception as e:
            log.info("Error creating model '{}' with schema '{}' in dataset '{}': {}".format(model, schema, ds.name, e))
    return model


### Parsing JSON data:
def getJson(_type):
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

def unitValue(node, name, default=None):
    'Convert a "unit+value" node to a string'
    if name in node:
        return '%s (%s)' % (', '.join(node[name]['value']), node[name]['unit'])
    return default

def getFirst(node, name, default=None):
    try:
        return node[name][0]
    except (KeyError, IndexError):
        return default


### Database I/O
#@mock_dynamodb2
def getDB():
    return boto3.resource('dynamodb', region_name=cfg.aws_region)

#@mock_dynamodb2
def getTable():
    if cfg.environment_name is not "test":
        return db.Table(cfg.table_id)
    return db.create_table(
        TableName=cfg.table_id,
        KeySchema=[
            {'AttributeName': cfg.table_partition_key, 'KeyType': "HASH"},
            {'AttributeName': cfg.table_sort_key, 'KeyType': "RANGE"}
        ],
        AttributeDefinitions=[
            {'AttributeName': cfg.table_partition_key, 'AttributeType': 'S'},
            {'AttributeName': cfg.table_sort_key, 'AttributeType': 'S'}
        ],
        BillingMode="PROVISIONED",
        ProvisionedThroughput={
            'ReadCapacityUnits': 123,
            'WriteCapacityUnits': 123
        }
    )

#@mock_dynamodb2
def buildCache(dsId):
    '''
    Get records cache from the database,
    return a dictionary of {model name: {identifier: record ID}}
    '''
    table = getTable()
    res = table.query(KeyConditionExpression=Key(cfg.table_partition_key).eq(dsId))
    cache = {m: {} for m in MODEL_NAMES}
    for item in res['Items']:
        cache[item['model']][item['identifier']] = item['recordId']
    log.debug('Retrieved {} database records for {}'.format(res['Count'], dsId))
    return cache

#@mock_dynamodb2
def writeCache(dsId, recordCache):
    'Write mappings in recordCache to the db'
    newEntries = [{
        'datasetId': dsId,
        'model': model,
        'identifier': k,
        'id': v}
        for model, records in recordCache.items() for k, v in records.items()]

    table = getTable()
    oldItems = table.query(KeyConditionExpression=Key(cfg.table_partition_key).eq(dsId))
    with table.batch_writer() as batch:
        for item in oldItems['Items']:
            try:
                batch.delete_item(Key={cfg.table_partition_key: dsId, cfg.table_sort_key: item['recordId']})
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
                    cfg.table_partition_key: e['datasetId'],
                    cfg.table_sort_key: e['id'],
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


### Removing data:
def deleteData(ds, models, recordCache, node):
    '''
    Delete records and/or record properties from a dataset
    '''
    if node['expired']:
        log.info('Untracking dataset')
        for k in recordCache:
            recordCache[k].clear()
        return
    for modelName, modelNode in node['records'].items():
        model = models[modelName]
        oldRecs = []
        for identifier, recNode in modelNode.items():
            try:
                recId = recordCache[modelName][identifier]
            except KeyError:
                log.warning('Tried to delete from a nonexistent record')
                continue
            if recNode['expired']:
                log.debug('Deleting record {}/{}'.format( modelName, identifier))
                oldRecs.append(identifier)
                continue
            record = model.get(recId)
            if recNode['values'] or recNode['arrayValues']:
                log.debug('Deleting properties of record {}'.format(record))
                removeProperties(ds, record, recordCache, recNode['values'], recNode['arrayValues'])
                try:
                    removeRecords(model, recordCache, *oldRecs)
                except:
                    log.info("Error trying to delete record {}".format(record))

def removeRecords(model, recordCache, *recordNames):
    'Remove record(s), but only if they exist both in the cache and on the platform'
    recIds = []
    for identifier in recordNames:
        try:
            recId = recordCache[model.type].pop(identifier)
        except KeyError:
            log.warning("removeRecords: record '{}/{}' not found in cache".format(model.type, identifier))
            continue
        else:
            recIds.append(recId)
    try:
        print("deleting '{}' record {}".format( model.type, str(recIds)))
        if not cfg.dry_run:
            model.delete_records(*recIds) # will print error message if a record doesn't exist
    except Exception as e:
        log.error("Failed to delete '{}' record(s): {}".format( model.type, str(recIds)))
        raise BlackfynnException(e)

def removeProperties(ds, record, recordCache, values, arrayValues):
    'Remove properties, linked properties and relationships on the platform'
    model = record.model
    ignoreProps = {
        'protocol': (),
        'researcher': (),
        'sample': ('hasDigitalArtifactThatIsAboutItHash'),
        'subject': ('localIdAlt'),
        'summary': (),
        'term': ('deprecated')}
    relnTypes = {
        'http://purl.obolibrary.org/obo/IAO_0000136': 'is-about',
        'protocolEmploysTechnique': 'protocol-employs-technique',
        'involvesAnatomicalRegion': 'involves-anatomical-region'}

    for v in values:
        if v in ignoreProps[model.type]:
            continue
        elif v in model.linked:
            try:
                record.delete_linked_value(v)
            except:
                log.info("Error trying to delete record linked value {}".format(v))
        else:
            record._set_value(v, None)
        try:
            print("deleting property '{}' from record {}".format(v, record))
            if not cfg.dry_run:
                record.update()
        except Exception as e:
            log.error("Failed to remove property '{}' from record {}".format(v, record))
            raise BlackfynnException(e)

    for prop, values in arrayValues.items():
        if prop in ignoreProps[model.type]:
            continue
        elif prop in relnTypes:
            rt = ds.get_relationship(relnTypes[prop])
            relationships = rt.get_all()
            for v in values:
                try:
                    targetId = recordCache['term'][v]
                except KeyError:
                    continue
                for r in relationships:
                    if r.source == record.id and r.destination == targetId:
                        r.delete()
        else:
            vals = record.get(prop)
            if vals is None:
                continue
            array = [x for x in vals if x not in values]
            if array == []:
                array = None
            try:
                print("editing property '{}' from record {}".format(prop, record))
                if not cfg.dry_run:
                    record.set(prop, array)
            except Exception as e:
                log.error("Failed to edit property '{}' of record '{}'".format(prop, record))
                raise BlackfynnException(e)

def get_packages(ds):
    packages = []
    for item in ds.items:
        packages.append(item)
        if isinstance(item, BaseCollection):
            packages += get_packages(item)
    return packages

### Adding data:
def addData(ds, dsId, recordCache, node, file):
    '''
    Add and/or update records in a dataset
    '''

    addProtocols(ds, recordCache, node['Protocols'], file)
    addTerms(ds, recordCache, node['Terms'], file)
    addResearchers(ds, recordCache, node['Researcher'], file)
    addSubjects(ds, recordCache, node['Subjects'], file)
    addSamples(ds, recordCache, node['Samples'], file)
    addAwards(ds, recordCache, dsId, node['Awards'], file)
    addSummary(ds, recordCache, dsId, node['Resource'], file)

def updateRecord(ds, recordCache, model, identifier, values, file, links=None, relationships=None):
    '''
    Create or update a record with the given properties/relationships
    model: Model object
    identifier: record identifier
    values: record's schema values
    links: linked values (structured {name: identifier})
    relationships: relationships(structured {name: [identifiers]})
    '''
    try:
        recId = recordCache[model.type][identifier]
    except KeyError:
        try:
            rec = model.create_record(values)
            log.debug('Created new record: {}'.format(rec))
            recordCache[model.type][identifier] = rec.id
        except Exception as e:
            log.error("Failed to create record with values {}".format(values))
            return None
    else:
        rec = model.get(recId)
        log.debug('Retrieved record from cache: {}'.format(rec))
        for prop, value in rec.values.items():
            if prop not in values or values[prop] in (None, '', '(no label)'):
                values[prop] = value
            elif isinstance(value, list):
                values[prop] = list(set(values[prop]).union(set(value)))
            file.append("adding property value '{}'='{}' to record {}".format(prop, value, rec))
        try:
            if not cfg.dry_run:
                rec._set_values(values)
                rec.update()
        except Exception as e:
            log.error("Failed to update values of record {}".format(rec))
            raise BlackfynnException(e)

    if links:
        addLinks(ds, recordCache, model, rec, links, file)
    if relationships:
        addRelationships(ds, recordCache, model, rec, relationships, file)
    return rec

def addLinks(ds, recordCache, model, record, links, file):
    'Add linked values to a record'
    for name, value in links.items():
        terms = None
        linkedProp = model.linked[name]
        targetType = ds.get_model(linkedProp.target).type
        if value in recordCache[targetType]:
            linkedRecId = recordCache[targetType][value]
        elif name == 'animalSubjectIsOfStrain':
            # create a new term for the animal strain
            if terms is None:
                terms = ds.get_model('term')
            linkedRecId = terms.create_record({'label': value}).id
            recordCache[targetType][value] = linkedRecId
        elif name == 'specimenHasIdentifier':
            # create a new identifier term
            if terms is None:
                terms = ds.get_model('term')
            linkedRecId = terms.create_record({'label': '(no label)', 'curie': value}).id
            recordCache[targetType][value] = linkedRecId
        else:
            log.warning("addLinks: Term with identifier '{}' not found for property '{}'".format(value, name))
            continue
        try:
            file.append("adding linked value '{}'='{}' to record {}".format(name, value, record))
            if not cfg.dry_run:
                record.add_linked_value(linkedRecId, linkedProp)
        except Exception as e:
            log.error("Failed to add linked value '{}'='{}' to record {} with error '{}'".format(name, value, record, str(e)))
            raise BlackfynnException(e)

def addRelationships(ds, recordCache, model, record, relationships, file):
    'Add relationships to a record'
    terms = ds.get_model('term')
    for name, values in relationships.items():
        if name in ('is-about', 'involves-anatomical-region', 'protocol-employs-technique'):
            try:
                rt = ds.create_relationship_type(name, description='',
                    source=model.id, destination=terms.id, display_name=name.replace('-', ' ').title())
            except:
                rt = ds.get_relationship(name)
            targets = []
            for v in values:
                if v in recordCache['term']:
                    targets.append(terms.get(recordCache['term'][v]))
                elif not v.isdigit():
                    target = terms.create_record({'label': v})
                    log.debug('addRelationships: created new record {}'.format(target))
                    recordCache['term'][v] = target.id
                    targets.append(target)
            try:
                file.append("adding '{}' relationship to record '{}'".format(rt.type, record))
                if not cfg.dry_run:
                    record.relate_to(targets, relationship_type=rt)
            except Exception as e:
                log.error("Failed to add '{}' relationship to record '{}'".format(rt.type, record))
                raise BlackfynnException(e)

#%% [markdown]
### Functions to update records of each model type
#%%
def addProtocols(ds, recordCache, subNode, file):
    log.info("Adding protocols...")
    model = getModel(ds, 'protocol', 'Protocol', schema=[
        ModelProperty('label', 'Name', title=True),
            ModelProperty('url', 'URL'),
            ModelProperty('protocolHasNumberOfSteps', 'Number of Steps'), # is this necessary?
            ModelProperty('hasNumberOfProtcurAnnotations', 'Number of Protcur Annotations') # is this necessary?
        ])
    for url, protocol in subNode.items():
        protocol['url'] = url
        prot = {k: protocol.get(k) for k in ('label', 'url', 'protocolHasNumberOfSteps', 'hasNumberOfProtcurAnnotations')}
        file.append("Adding protocols '{}' to dataset '{}'".format(prot, ds))

        if not cfg.dry_run:
                updateRecord(ds, recordCache, model, url, prot, file)

def addTerms(ds, recordCache, subNode, file):
    log.info("Adding terms...")
    model = getModel(ds, 'term', 'Term', schema=[
        ModelProperty('label', 'Label', title=True), # is a list
            ModelProperty('curie', 'CURIE'),
            ModelProperty('definitions', 'Definition'), # is a list
            ModelProperty('abbreviations', 'Abbreviations', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # is a list
            ModelProperty('synonyms', 'Synonyms', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # is a list
            ModelProperty('acronyms', 'Acronyms', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # is a list
            ModelProperty('categories', 'Categories', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # is a list
            ModelProperty('iri', 'IRI'),
        ])

    def transform(term):
        return {
            'label': getFirst(term, 'labels', '(no label)'),
            'curie': term.get('curie'),
            'definitions': getFirst(term, 'definitions'),
            'abbreviations': term.get('abbreviations'),
            'synonyms': term.get('synonyms'),
            'acronyms': term.get('acronyms'),
            'categories': term.get('categories'),
            'iri': term.get('iri'),
        }

    tags = []
    for curie, term in subNode.items():
        file.append("Adding term '{}' to dataset '{}'".format(transform(term), ds))
        updateRecord(ds, recordCache, model, curie, transform(term), file)
        tags.append(getFirst(term, 'labels'))
    ds.tags=list(set(tags+ds.tags))
    ds.update()

def addResearchers(ds, recordCache, subNode, file):
    log.info("Adding researchers...")

    model = getModel(ds, 'researcher', 'Researcher', schema=[
            ModelProperty('lastName', 'Last name', title=True),
            ModelProperty('firstName', 'First name'),
            ModelProperty('middleName', 'Middle name', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # list
            ModelProperty('hasAffiliation', 'Affiliation', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # list
            ModelProperty('hasRole', 'Role', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # list
            ModelProperty('hasORCIDId', 'ORCID iD')
    ])

    def transform(subNode):
        return {
            'lastName': subNode.get('lastName', '(no label)'),
            'firstName': subNode.get('firstName'),
            'middleName': subNode.get('middleName'),
            'hasAffiliation': subNode.get('hasAffiliation'),
            'hasRole': subNode.get('hasRole'),
            'hasORCIDId': subNode.get('hasORCIDId')
        }

    for userId, researcher in subNode.items():
        file.append("Adding researcher '{}' to dataset '{}'".format(transform(researcher), ds))
        updateRecord(ds, recordCache, model, userId, transform(researcher), file)

def addSubjects(ds, recordCache, subNode, file):
    log.info("Adding subjects...")
    termModel = ds.get_model('term')
    model = getModel(ds, 'subject', 'Subject',
        schema=[
            ModelProperty('localId', 'Subject ID', title=True),
            ModelProperty('animalSubjectHasWeight', 'Animal weight'), # unit+value
            ModelProperty('subjectHasWeight', 'Weight'), # unit+value
            ModelProperty('subjectHasHeight', 'Height'), # unit+value
            ModelProperty('hasAge', 'Age'), # unit+value
            ModelProperty('protocolExecutionDate', 'Protocol execution date', data_type=ModelPropertyEnumType(
                data_type='date', multi_select=True)), # list of MM-DD-YY strings
            ModelProperty('localExecutionNumber', 'Execution number', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # list
            ModelProperty('hasAssignedGroup', 'Group', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # list
            ModelProperty('spatialLocationOfModulator', 'Spatial location of modulator', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # list
            ModelProperty('stimulatorUtilized', 'Stimulator utilized'),
            ModelProperty('providerNote', 'Provider note', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # list
            #ModelProperty('localIdAlt', 'Alternate local id'),
            ModelProperty('hasGenotype', 'Genotype'),
            ModelProperty('raw/involvesAnatomicalRegion', 'Anatomical region involved'),
            ModelProperty('wasAdministeredAnesthesia', 'Anesthesia administered'),
        ], linked=[
            LinkedModelProperty('animalSubjectIsOfSpecies', termModel, 'Animal species'),
            LinkedModelProperty('animalSubjectIsOfStrain', termModel, 'Animal strain'),
            LinkedModelProperty('hasBiologicalSex', termModel, 'Biological sex'), # list (this is a bug)
            LinkedModelProperty('hasAgeCategory', termModel, 'Age category'),
            LinkedModelProperty('specimenHasIdentifier', termModel, 'Identifier'),
        ])
    linkedProperties = model.linked.values()

    def transform(subNode, localId):
        vals = {
            'localId': localId,
            'localExecutionNumber': subNode.get('localExecutionNumber'),
            'subjectHasWeight': unitValue(subNode, 'subjectHasWeight'),
            'subjectHasHeight': unitValue(subNode, 'subjectHasHeight'),
            'hasAge': unitValue(subNode, 'hasAge'),
            'spatialLocationOfModulator': subNode.get('spatialLocationOfModulator'),
            'stimulatorUtilized': subNode.get('stimulatorUtilized'),
            'hasAssignedGroup': subNode.get('hasAssignedGroup'),
            'providerNote': subNode.get('providerNote'),
            'raw/involvesAnatomicalRegion': subNode.get('raw/involvesAnatomicalRegion'),
            'hasGenotype': subNode.get('hasGenotype'),
            'animalSubjectHasWeight': unitValue(subNode, 'animalSubjectHasWeight'),
            'wasAdministeredAnesthesia': subNode.get('wasAdministeredAnesthesia')
        }
        try:
            vals['protocolExecutionDate'] = [DT.strptime(x, '%m-%d-%y') for x in subNode['protocolExecutionDate']]
        except (ValueError, KeyError):
            # date is either not given or formatted wrong
            vals['protocolExecutionDate'] = None
        return vals

    for subjId, subjNode in subNode.items():
        links = {}
        for prop in linkedProperties:
            if prop.name in subjNode:
                if prop.name == 'hasBiologicalSex':
                    # ~~BUG: biological sex is a list~~
                    value = subjNode[prop.name]
                else:
                    value = subjNode[prop.name]
                links[prop.name] = value
        file.append("Adding subject '{}' to dataset '{}'".format( subjId, ds))
        try:
            updateRecord(ds, recordCache, model, subjId, transform(subjNode, subjId), file, links)
        except Exception as e:
            log.error("Addition of subject '{}' failed because of {}".format(subjId, e))

def contains(list, filter):
    for x in list:
        if filter(x):
            return x
    return False

def addSamples(ds, recordCache, subNode, file):
    log.info("Adding samples...")
    model = getModel(ds, 'sample', 'Sample',
        schema=[
            ModelProperty('localId', 'ID', title=True),
            ModelProperty('label', 'Label'),
            ModelProperty('description', 'Description'), # list
            ModelProperty('hasAssignedGroup', 'Group', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # list
            ModelProperty('hasDigitalArtifactThatIsAboutIt', 'Digital artifact', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # filename list
            #ModelProperty('hasDigitalArtifactThatIsAboutItHash', ), # list
            ModelProperty('localExecutionNumber', 'Execution number', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # list
            ModelProperty('providerNote', 'Provider note', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # list
        ], linked=[
            LinkedModelProperty('raw/wasExtractedFromAnatomicalRegion', ds.get_model('term'), 'Extracted from anatomical region'),
            LinkedModelProperty('wasDerivedFromSubject', ds.get_model('subject'), 'Derived from subject')
        ])
    anatomicalRegionLink = model.linked['raw/wasExtractedFromAnatomicalRegion']
    fromSubjectLink = model.linked['wasDerivedFromSubject']


    def transform(subNode):
        return {
            'localId': subNode.get('localId', '(no label)'),
            'description': getFirst(subNode, 'description'),
            'hasAssignedGroup': subNode.get('hasAssignedGroup'),
            'hasDigitalArtifactThatIsAboutIt': subNode.get('hasDigitalArtifactThatIsAboutIt'),
            #'hasDigitalArtifactThatIsAboutItHash': subNode.get('hasDigitalArtifactThatIsAboutItHash'),
            'label': subNode.get('label'),
            'localExecutionNumber': subNode.get('localExecutionNumber'),
            'providerNote': subNode.get('providerNote')
        }

    regex = re.compile(r'.*/subjects/(.+)')
    for sampleId, subNode in subNode.items():
        # get linked values:
        links = {}
        if 'raw/wasExtractedFromAnatomicalRegion' in subNode:
            links['raw/wasExtractedFromAnatomicalRegion'] = subNode['raw/wasExtractedFromAnatomicalRegion']
        if 'wasDerivedFromSubject' in subNode:
            identifier = regex.match(subNode['wasDerivedFromSubject']).group(1)
            if identifier in recordCache['subject']:
                links['wasDerivedFromSubject'] = identifier

        file.append("Adding sample '{}' to dataset '{}'".format(transform(subNode), ds))
        rec = updateRecord(ds, recordCache, model, sampleId, transform(subNode), file, links)

        if subNode.get('hasDigitalArtifactThatIsAboutIt') is not None:
            for fullFileName in subNode.get('hasDigitalArtifactThatIsAboutIt'):
                filename, file_extension = os.path.splitext(fullFileName)
                pkgs = ds.get_packages_by_filename(filename)
                if len(pkgs) > 0:
                    for pkg in pkgs:
                        pkg.relate_to(rec)


def addSummary(ds, recordCache, identifier, subNode, file):
    log.info("Adding summary...")

    termModel = ds.get_model('term')
    model = getModel(ds, 'summary', 'Summary', schema=[
        ModelProperty('title', 'Title', title=True), # list
        ModelProperty('hasResponsiblePrincipalInvestigator', 'Responsible Principal Investigator',
                      data_type=ModelPropertyEnumType(data_type=str, multi_select=True)),
        # list of ORCID URLs, blackfynn user IDs, and, and Blackfynn contributor URLs
        # TODO: make this a relationship?
        ModelProperty('isDescribedBy', 'Publication URL', data_type=ModelPropertyEnumType(
            data_type=str, multi_select=True)), # list (of urls)
        ModelProperty('description', 'Description', data_type=ModelPropertyEnumType(
            data_type=str, multi_select=True)), # list
        # TODO: update dataset description using PUT /datasets/{id}/readme
        ModelProperty('collectionTitle', 'Collection'),
        ModelProperty('curationIndex', 'Curation index'), # number string
     #   ModelProperty('hasAwardNumber', 'Award number'),
        ModelProperty('hasExperimentalModality', 'Experimental modality', data_type=ModelPropertyEnumType(
            data_type=str, multi_select=True)), # list
        ModelProperty('hasNumberOfContributors', 'Number of contributors'), # number string
        ModelProperty('hasNumberOfDirectories', 'Number of directories'), # number string
        ModelProperty('hasNumberOfFiles', 'Number of files'), # number string
        ModelProperty('hasNumberOfSamples', 'Number of samples'), # number string
        ModelProperty('hasNumberOfSubjects', 'Number of subjects'), # number string
        ModelProperty('acknowledgements', 'Acknowledgements'),
        ModelProperty('submissionIndex', 'Submission index'), # number string
        ModelProperty('errorIndex', 'Error index'), # number string
        ModelProperty('label', 'Label'),
        ModelProperty('hasSizeInBytes', 'Size (bytes)'), # number string
    ], linked=[
        LinkedModelProperty('hasAwardNumber', ds.get_model('award'), 'Award number'),
    ])
    hasAwardNumber = model.linked['hasAwardNumber']

    def transform(subNode, description, isDescribedBy, hasExperimentalModality, hasResponsiblePrincipalInvestigator):
        return {
            'isDescribedBy': isDescribedBy,
            'acknowledgements': subNode.get('acknowledgements'),
            'collectionTitle': subNode.get('collectionTitle'),
            'curationIndex': subNode.get('curationIndex'),
            'description': description,
            'errorIndex': subNode.get('errorIndex'),
          #  'hasAwardNumber': subNode['hasAwardNumber'] if 'hasAwardNumber' in subNode else None,
            'hasExperimentalModality': hasExperimentalModality,
            'hasNumberOfContributors': subNode.get('hasNumberOfContributors'),
            'hasNumberOfDirectories': subNode.get('hasNumberOfDirectories'),
            'hasNumberOfFiles': subNode.get('hasNumberOfFiles'),
            'hasNumberOfSamples': subNode.get('hasNumberOfSamples'),
            'hasNumberOfSubjects': subNode.get('hasNumberOfSubjects'),
            'hasResponsiblePrincipalInvestigator': hasResponsiblePrincipalInvestigator,
            'hasSizeInBytes': subNode.get('hasSizeInBytes'),
            'label': subNode.get('label'),
            'submissionIndex': subNode.get('submissionIndex'),
            'title': getFirst(subNode, 'title', default=subNode.get('label', '(no label)')),
        }

    if 'hasResponsiblePrincipalInvestigator' in subNode:
        if isinstance(subNode.get('hasResponsiblePrincipalInvestigator'), list):
            hasResponsiblePrincipalInvestigator = subNode.get('hasResponsiblePrincipalInvestigator')
        else:
            hasResponsiblePrincipalInvestigator = [subNode.get('hasResponsiblePrincipalInvestigator')]
    else:
        hasResponsiblePrincipalInvestigator = None

    if 'hasExperimentalModality' in subNode:
        if isinstance(subNode.get('hasExperimentalModality'), list):
            hasExperimentalModality = subNode.get('hasExperimentalModality')
        else:
            hasExperimentalModality = [subNode.get('hasExperimentalModality')]
    else:
        hasExperimentalModality = None

    if 'isDescribedBy' in subNode:
        if isinstance(subNode.get('isDescribedBy'), list):
            isDescribedBy = subNode.get('isDescribedBy')
        else:
            isDescribedBy = [subNode.get('isDescribedBy')]
    else:
        isDescribedBy = None

    if 'description' in subNode:
        if isinstance(subNode.get('description'), list):
            description = subNode.get('description')
        else:
            description = [subNode.get('description')]
    else:
        description = None

    links = {}

    relations = {}
    # get "is about" relationships

    links['hasAwardNumber'] = subNode['hasAwardNumber'] if ('hasAwardNumber' in subNode and subNode['hasAwardNumber'] in recordCache['award']) else None

    regex = re.compile(r'\w+:\w+')
    for value in subNode.get('http://purl.obolibrary.org/obo/IAO_0000136', []):
        if regex.match(value):
            relations.setdefault('is-about', []).append(value)

    # get "involves anatomical region" relationships
    for value in subNode.get('involvesAnatomicalRegion', []):
        relations.setdefault('involves-anatomical-region', []).append(value)

    # get "protocol employs technique" relationships
    for value in subNode.get('protocolEmploysTechnique', []):
        relations.setdefault('protocol-employs-technique', []).append(value)

    file.append("Adding summary '{}' to dataset '{}'".format(transform(subNode, description, isDescribedBy, hasExperimentalModality, hasResponsiblePrincipalInvestigator), ds))
    try:
        updateRecord(ds, recordCache, model, identifier, transform(subNode, description, isDescribedBy, hasExperimentalModality, hasResponsiblePrincipalInvestigator), file, relationships=relations, links=links)
    except Exception as e:
        log.error("Failed to add summary to dataset '{}'".format(ds))


def addAwards(ds, recordCache, identifier, subNode, file):
    log.info("Adding awards...")

    model = getModel(ds, 'award', 'Award', schema=[
        ModelProperty('award_id', 'Award ID', title=True),
        ModelProperty('title', 'Title'),
        ModelProperty('description', 'Description'),
        ModelProperty('principal_investigator', 'Principal Investigator'),

    ])

    def transform(awardId):
        r = requests.get(url = u'https://api.federalreporter.nih.gov/v1/projects/search?query=projectNumber:*{}*'.format(awardId))
        try:
            data = r.json()
        except Exception as e:
            return {
                'award_id': awardId,
                'title': None,
                'description': None,
                'principal_investigator': None,
            }
        if data['totalCount'] > 0:
            return {
                'award_id': awardId,
                'title': data['items'][0]['title'],
                'description': data['items'][0]['abstract'],
                'principal_investigator': data['items'][0]['contactPi'],

            }
        else:
            return {
                'award_id': awardId,
                'title': None,
                'description': None,
                'principal_investigator': None,
            }

    for awardId, _ in subNode.items():
        file.append("Adding award '{}' to dataset '{}'".format(transform(awardId), ds))
        updateRecord(ds, recordCache, model, awardId, transform(awardId), file)

#%% [markdown]
### Main body
#%%
def update(datasetIds, reset=False):
    '''
    Only update the given datasets.
    if `reset`: clear and re-add all records.

    Returns: list of datasets that failed to update
    '''
    log.info('Updating specified datasets:')
    log.debug('Datasets to update: %s', ' '.join(datasetIds))
    if reset:
        oldJson = {}
        newJson = getJson('full')
    else:
        oldJson, newJson = getJson('diff')

    failedDatasets = []
    for dsId in datasetIds:
        log.info('Current dataset: %s', dsId)
        if dsId in SKIP_LIST or dsId in failedDatasets:
            log.info('skipping...')
            continue
        if not authorized(dsId):
            log.warning('Skipping update: "UNAUTHORIZED: {}"'.format(dsId))
            continue

        ds = getDataset(dsId)
        if reset:
            clearDataset(ds)
            recordCache = {m: {} for m in MODEL_NAMES}
        else:
            recordCache = buildCache(dsId)
        models = {k: v for k, v in ds.models().items() if k in MODEL_NAMES}

        deleteNode = next((v for k, v in oldJson.items() if k == dsId), None)
        addNode = next((v for k, v in newJson.items() if k == dsId), None)
        if not (addNode or deleteNode):
            log.info('Nothing to update. Skipping...')
            continue

        if deleteNode:
            log.debug('Deleting old metadata...')
            try:
                deleteData(ds, models, recordCache, deleteNode)
            except BlackfynnException:
                log.error("Dataset %s failed to update", dsId)
                failedDatasets.append(dsId)
                continue
            finally:
                writeCache(dsId, recordCache)
        if addNode:
            log.debug('Adding new metadata...')
            try:
                addData(ds, dsId, recordCache, addNode)
            except BlackfynnException:
                failedDatasets.append(dsId)
                continue
            finally:
                writeCache(dsId, recordCache)
    return failedDatasets

def updateAll(reset=False):
    '''
    Update all datasets.
    if `reset`: clear and re-add all records.

    Returns: list of datasets that failed to update
    '''
    log.info('Updating all datasets:')
    update_start_time = time()

    if reset:
        oldJson = {}
        newJson = getJson('full')
    else:
        oldJson, newJson = getJson('diff')

    failedDatasets = []
    instructionList = []
    log.info('=== Deleting old metadata ===')
    delete_start_time = time()

    for dsId, node in oldJson.items():
        log.info('Current dataset: {}'.format(dsId))
        if dsId in SKIP_LIST:
            log.info('skipping...')
            continue
        if not authorized(dsId):
            log.warning('Skipping deleting old metadata: "UNAUTHORIZED: {}"'.format(dsId))
            continue

        ds = getDataset(dsId)
        if reset:
            clearDataset(ds)
            recordCache = {m: {} for m in MODEL_NAMES}
        else:
            recordCache = buildCache(dsId)
        models = {k: v for k, v in ds.models().items() if k in MODEL_NAMES}
        try:
            deleteData(ds, models, recordCache, node)
        except BlackfynnException:
            log.error("Dataset {} failed to update".format(dsId))
            failedDatasets.append(dsId)
            continue
        finally:
            writeCache(dsId, recordCache)

    duration = int((time() - delete_start_time) * 1000)
    log.info("Deleted old metadata in {} milliseconds".format(duration))

    log.info('===========================')
    log.info('=== Adding new metadata ===')
    log.info('===========================')
    log.info('')
    new_start_time = time()
    for dsId, node in newJson.items():
        log.info('Current dataset: {}'.format(dsId))
        instructionList.append('Current dataset: {}'.format(dsId))
        if dsId in SKIP_LIST or dsId in failedDatasets:
            log.info('skipping...')
            continue
        if not authorized(dsId):
            log.warning('Skipping adding new metadata: "UNAUTHORIZED: {}"'.format(dsId))
            continue

        ds = getDataset(dsId)
        if reset:
            clearDataset(ds)
            recordCache = {m: {} for m in MODEL_NAMES}
        else:
            recordCache = buildCache(dsId)
        try:
            addData(ds, dsId, recordCache, node, instructionList)
        except BlackfynnException:
            log.error('Dataset {} failed to update'.format(dsId))
            failedDatasets.append(dsId)
            continue
        finally:
            writeCache(dsId, recordCache)

    duration = int((time() - new_start_time) * 1000)
    log.info("Added new metadata in {} milliseconds".format(duration))

    duration = int((time() - update_start_time) * 1000)
    log.info("Update datasets in {} milliseconds".format(duration))

    with open(INSTRUCTION_FILE, 'w') as f:
        for item in instructionList:
            f.write("%s\n" % item)
        f.close()
    return failedDatasets

def update_sparc_dataset():
    sparc_ds = getDataset(SPARC_DATASET_ID)
    model = sparc_ds.get_model('Update_Run')
    model.create_record({'name':'TTL Update', 'status': DT.now()})


bf = bfClient()
db = getDB()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        update(sys.argv[1:])
    else:
        updateAll()

    log.info('Update finished.')
