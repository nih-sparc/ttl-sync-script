from datetime import datetime as DT
import json
import logging
import re
import sys
import os
import requests
from blackfynn.models import ModelPropertyEnumType, BaseCollection, ModelPropertyType
from blackfynn import Blackfynn, ModelProperty, LinkedModelProperty

from time import time
from bf_io import (
    authorized,
    getCreateDataset,
    clearDataset,
    getModel,
    BlackfynnException,
    update_sparc_dashboard
)

from base import (
    JSON_METADATA_EXPIRED,
    JSON_METADATA_FULL,
    JSON_METADATA_NEW,
    SPARC_DATASET_ID,
    SSMClient,
    MODEL_NAMES
)
from pprint import pprint

logging.basicConfig(format="%(asctime);s%(filename)s:%(lineno)d:\t%(message)s")
log = logging.getLogger(__name__)

### Helper function to check li
def contains(list, filter):
    for x in list:
        if filter(x):
            return x
    return False

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

def get_record_by_id(json_id, model, record_cache):
    """Get Blackfynn Record by its JSON ID

        The JSON_ID should be DatasetID_ModelName_RecordName

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

def unitValue(node, name, model_unit = 'None'):
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

def getFirst(node, name, default=None):
    try:
        return node[name][0]
    except (KeyError, IndexError):
        return default

### Removing data: Delete specific records from dataset
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
                record.set(prop, array)
            except Exception as e:
                log.error("Failed to edit property '{}' of record '{}'".format(prop, record))
                raise BlackfynnException(e)

### Get array of all packages, including nested packages
def get_packages(ds):
    packages = []
    for item in ds.items:
        packages.append(item)
        if isinstance(item, BaseCollection):
            packages += get_packages(item)
    return packages

### Adding data:
def addData(bf, ds, dsId, recordCache, node):
    '''
    Add and/or update records in a dataset
    '''

    # Get Models
    models = ds.models()

    # Adding all records without setting linked properties and relationships
    addProtocols(bf, ds, recordCache, node['Protocols'])
    addTerms(bf,ds, recordCache, node['Terms'])
    addResearchers(bf,ds, recordCache, node['Researcher'])
    addSubjects(bf,ds, recordCache, node['Subjects'])
    addSamples(bf,ds, recordCache, node['Samples'])
    addAwards(bf,ds, recordCache, dsId, node['Awards'])
    addSummary(bf,ds, recordCache, dsId, node['Resource'])

def addLinks(bf, ds, dsId, recordCache, node):

    '''
    Add and/or update records in a dataset
    '''

    
    # Adding all records without setting linked properties and relationships
    # addProtocols(bf, ds, recordCache, node['Protocols'])
    # addTerms(bf,ds, recordCache, node['Terms'])
    # addResearchers(bf,ds, recordCache, node['Researcher'])
    addSubjectLinks(bf, ds, recordCache, node['Subjects'])
    # addSamples(bf,ds, recordCache, node['Samples'])
    # addAwards(bf,ds, recordCache, dsId, node['Awards'])
    # addSummary(bf,ds, recordCache, dsId, node['Resource'])

def addRandomTerms(ds, label, recordCache):
    """Adding a record for a term that is not defined in TTL

    Most terms are defined in the TTL file as entities. However
    some are not and are not populated before iterating over the 
    other entities. This method adds the term as a record to the 
    TERM model.

    Parameters
    ----------
    ds: BF_Dataset
        Dataset that contains the records
    label: str
        Label for new term
    recordCache: Dict
        Dictionary mapping record identifier to record

    """

    if not hasattr(addRandomTerms, "term_model"):
        addRandomTerms.term_model = ds.get_model('term')
        addRandomTerms.model_ds = ds.id
    elif addRandomTerms.model_ds != ds.id:
        addRandomTerms.term_model = ds.get_model('term')
        addRandomTerms.model_ds = ds.id
 
    # terms = ds.get_model('term')
    term_id = addRandomTerms.term_model.create_record({'label': label}).id
    recordCache['term'][label] = term_id
    return term_id

def updateRecord(ds, recordCache, model, identifier, values, links=None, relationships=None):
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
            log.info('Created new record: {}'.format(rec))
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
        try:
            rec._set_values(values)
            log.info('updating record')
            rec.update()
        except Exception as e:
            log.error("Failed to update values of record {}".format(rec))
            raise BlackfynnException(e)

    # if links:
    #     addLinks(ds, recordCache, model, rec, links)
    # if relationships:
    #     addRelationships(ds, recordCache, model, rec, relationships)
    return rec

def addRecordLinks(ds, recordCache, model, record, links):
    """Populate linked Properties for single record

    This method populates linked properties in a record provided to method.

    Parameters
    ----------
    ds: BF_Dataset
        Dataset that contains the records
    recordCache: Dict
        Dictionary mapping record identifier to record
    model: BF_Model
        Model of the record that is updated
    record: BF_Record
        Record that is being updated
    links: Array [ {name:  Node }]
        linked values (structured {name: identifier})

    """
    log.info('LINKS: {}'.format(links))
    for name, value in links.items():
        # name: name of property to add, 
        # value = value of property ("id, or array of id's ")

        valueList = None
        if isinstance(value, str):
            valueList = [value]
        elif isinstance(value, list):
            valueList = value
        elif value == None:
            continue
        else:
            raise(Exception('Incorrect type for links.'))

        terms = None
        linkedProp = model.linked[name]

        # Find model name of the linked property target
        log.info('{}'.format(linkedProp.target))
        targetType = ds.get_model(linkedProp.target).type

        for item in valueList:
            # Check if value is in the record cache
            if item in recordCache[targetType]:
                # Record in cache --> exists in platform as a record
                linkedRecId = recordCache[targetType][item]
            else:
                # Record not in cache --> check if term --> if so, add new term,
                # if not --> throw warning and don't link entry
                if targetType == 'term':
                    linkedRecId = addRandomTerms(ds, item, recordCache)
                else:
                    log.warning('Unable to link to non-existing record {}'.format(targetType.target))
                    continue
            
            # Try to link record to property
            try:
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
                record.relate_to(targets, relationship_type=rt)
            except Exception as e:
                log.error("Failed to add '{}' relationship to record '{}'".format(rt.type, record))
                raise BlackfynnException(e)

#%% [markdown]
### Functions to update records of each model type
#%%
def addProtocols(bf, ds, recordCache, subNode):
    log.info("Adding protocols...")

    protocol_links = {}
    model = getModel(bf, ds, 'protocol', 'Protocol', schema=[
        ModelProperty('label', 'Name', title=True),
        ModelProperty('url', 'URL',data_type=ModelPropertyType(
                data_type=str, format='url')),
        ModelProperty('protocolHasNumberOfSteps', 'Number of Steps'), 
        ModelProperty('hasNumberOfProtcurAnnotations', 'Number of Protcur Annotations')
    ])
    record_list = []
    for url, protocol in subNode.items():
        protocol['url'] = url
        record_list.append({k: protocol.get(k) for k in ('label', 'url', 'protocolHasNumberOfSteps', 'hasNumberOfProtcurAnnotations')})
    
    log.info('Creating {} new records'.format(len(record_list)))
    if len(record_list):
        recs = model.create_records(record_list)

    return protocol_links

def addTerms(bf, ds, recordCache, subNode):
    log.info("Adding terms...")
    model = getModel(bf, ds, 'term', 'Term', schema=[
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
            ModelProperty('iri', 'IRI')
        ]
    )
        
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
    # record_list = []
    term_record_list = []
    term_json_id_list = []
    for curie, term in subNode.items():
        # record_list.append(transform(term))
        # updateRecord(ds, recordCache, model, curie, transform(term))
        tags.append(getFirst(term, 'labels'))
        term_record_list.append(transform(term))
        term_json_id_list.append("{}".format(curie))

    log.info('Creating {} new records'.format(len(term_record_list)))
    if len(term_record_list):
        recordCache['term'].update(zip(term_json_id_list, model.create_records(term_record_list)))

        # recs = model.create_records(record_list)

    ds.tags=list(set(tags+ds.tags))
    ds.update()

def addResearchers(bf, ds, recordCache, subNode):
    log.info("Adding researchers...")

    model = getModel(bf, ds, 'researcher', 'Researcher', schema=[
            ModelProperty('lastName', 'Last name', title=True),
            ModelProperty('firstName', 'First name'),
            ModelProperty('middleName', 'Middle name'),
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

    record_list = []
    for userId, researcher in subNode.items():
        record_list.append(transform(researcher))
        # updateRecord(ds, recordCache, model, userId, transform(researcher))
    
    log.info('Creating {} new records'.format(len(record_list)))
    if len(record_list):
        recs = model.create_records(record_list)    

def addSubjects(bf, ds, recordCache, subNode):
    log.info("Adding subjects...")
    termModel = ds.get_model('term')

    ## Define Model Generators
    def create_human_model(bf, ds):
        return getModel(bf, ds, 'human_subject', 'Human Subject',
            schema = [
                ModelProperty('localId', 'Subject ID', title=True),
                ModelProperty('subjectHasWeight', 'Weight', data_type=ModelPropertyType(
                    data_type=float, unit='g' )), # unit+value
                ModelProperty('subjectHasHeight', 'Height'), # unit+value
                ModelProperty('hasAge', 'Age',data_type=ModelPropertyType(
                    data_type=float, unit='s' )), # unit+value
                ModelProperty('hasAssignedGroup', 'Group', data_type=ModelPropertyEnumType(
                    data_type=str, multi_select=True)), # list
                ModelProperty('spatialLocationOfModulator', 'Spatial location of modulator', data_type=ModelPropertyEnumType(
                    data_type=str, multi_select=True)), # list
                ModelProperty('stimulatorUtilized', 'Stimulator utilized'),
                ModelProperty('providerNote', 'Provider note', data_type=ModelPropertyEnumType(
                    data_type=str, multi_select=True)), # list
                ModelProperty('hasGenotype', 'Genotype'),
                ModelProperty('raw/involvesAnatomicalRegion', 'Anatomical region involved'),
                ModelProperty('wasAdministeredAnesthesia', 'Anesthesia administered'),
            ], linked=[
                LinkedModelProperty('hasBiologicalSex', termModel, 'Biological sex'), # list (this is a bug)
                LinkedModelProperty('hasAgeCategory', termModel, 'Age category'),
                LinkedModelProperty('specimenHasIdentifier', termModel, 'Identifier'),
            ]
            )

    def create_animal_model(bf, ds):
        return getModel(bf, ds, 'animal_subject', 'Animal Subject',
            schema=[
                ModelProperty('localId', 'Subject ID', title=True),
                ModelProperty('animalSubjectHasWeight', 'Animal weight'), # unit+value
                ModelProperty('hasAge', 'Age',data_type=ModelPropertyType(
                    data_type=float, unit='s' )), # unit+value
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

    ## Define Transform methods
    def transform_human(subNode, localId):
        vals = {
            'localId': localId,
            'localExecutionNumber': subNode.get('localExecutionNumber'),
            'subjectHasWeight': unitValue(subNode, 'subjectHasWeight', 'kg'),
            'subjectHasHeight': unitValue(subNode, 'subjectHasHeight'),
            'hasAge': unitValue(subNode, 'hasAge', 's'),
            'spatialLocationOfModulator': subNode.get('spatialLocationOfModulator'),
            'stimulatorUtilized': subNode.get('stimulatorUtilized'),
            'hasAssignedGroup': subNode.get('hasAssignedGroup'),
            'providerNote': subNode.get('providerNote'),
            'raw/involvesAnatomicalRegion': subNode.get('raw/involvesAnatomicalRegion'),
            'hasGenotype': subNode.get('hasGenotype'),
            'wasAdministeredAnesthesia': subNode.get('wasAdministeredAnesthesia')
        }

        return vals
    
    def transform_animal(subNode, localId):
        vals = {
            'localId': localId,
            'localExecutionNumber': subNode.get('localExecutionNumber'),
            'hasAge': unitValue(subNode, 'hasAge', 's'),
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

    ## Separate human/animal subjects
    human_record_list = []
    human_json_id_list = []
    animal_record_list = []
    animal_json_id_list = []
    human_model = None
    animal_model = None
    human_recs = None
    animal_recs = None

    # Iterate over all subjects in a single dataset
    for subjId, subjNode in subNode.items():
        subtype = subNode.get('animalSubjectIsOfSpecies')
        if subtype == 'homo sapiens':
            human_record_list.append(transform_human(subjNode, subjId))
            human_json_id_list.append("{}".format(subjId))
        else:
            animal_record_list.append(transform_animal(subjNode, subjId))
            animal_json_id_list.append("{}".format( subjId))
    
    ## Create records
    if len(human_record_list) > 0:
        log.info('Creating {} new Human Subject Records'.format(len(human_record_list)))
        human_model = create_human_model(bf, ds)
        recordCache['human_subject'].update(zip(human_json_id_list,human_model.create_records(human_record_list)))

    elif len(animal_record_list) > 0:
        log.info('Creating {} new Animal Subject Records'.format(len(animal_record_list)))
        animal_model = create_animal_model(bf, ds)
        recordCache['animal_subject'].update(zip(animal_json_id_list,animal_model.create_records(animal_record_list)))

def addSubjectLinks(bf, ds, recordCache, subNode): 

    model = None
    subtype = subNode.get('animalSubjectIsOfSpecies')
    if subtype == 'homo sapiens':
        model = ds.get_model('human_subject')
    else:
        model = ds.get_model('animal_subject')

    def transform_human(subNode, localId):
        links = {
            'hasBiologicalSex': subNode.get('hasBiologicalSex'),
            'hasAgeCategory': subNode.get('hasAgeCategory'),
            'specimenHasIdentifier':subNode.get('specimenHasIdentifier')
        }
        return links

    def transform_animal(subNode, localId):
        links = {
            'animalSubjectIsOfSpecies': subNode.get('animalSubjectIsOfSpecies'),
            'animalSubjectIsOfStrain': subNode.get('animalSubjectIsOfStrain'),
            'hasBiologicalSex': subNode.get('hasBiologicalSex'),
            'hasAgeCategory': subNode.get('hasAgeCategory'),
            'specimenHasIdentifier':subNode.get('specimenHasIdentifier')
        }
        return links

    # Iterate over multiple subject records, single dataset
    for subjectId, subjNode in subNode.items():
        record = get_record_by_id(subjectId, model, recordCache)

        if subtype == 'homo sapiens':
            links = transform_human(subjNode, subjectId)
        else:
            links = transform_animal(subjNode, subjectId)
            
        addRecordLinks(ds, recordCache, model, record, links)

def addSamples(bf, ds, recordCache, subNode):
    log.info("Adding samples...")

    # Check if Human or Animal Subjects in Model or create new 
    # generic model to support linked property "derivedFromSubject"
    # Assuming no datasets with both human, and animal subjects
    models = ds.models()
    subModel = None
    if 'human_subject' in models:
        subModel = models['human_subject']
    elif 'animal_subject' in models:
        subModel = models['animal_subject']
    else:
        subModel = getModel(bf, ds, 'subject', 'Subject',
            schema=[
                ModelProperty('localId', 'ID', title=True)
            ]
            )

    # Get/Create the Sample model
    model = getModel(bf, ds, 'sample', 'Sample',
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
            LinkedModelProperty('wasDerivedFromSubject', subModel, 'Derived from subject')
        ])
    anatomicalRegionLink = model.linked['raw/wasExtractedFromAnatomicalRegion']
    fromSubjectLink = model.linked['wasDerivedFromSubject']

    def transform(subNode):
        return {
            'localId': subNode.get('localId', '(no label)'),
            'description': getFirst(subNode, 'description'),
            'hasAssignedGroup': subNode.get('hasAssignedGroup'),
            'hasDigitalArtifactThatIsAboutIt': subNode.get('hasDigitalArtifactThatIsAboutIt'),
            'label': subNode.get('label'),
            'localExecutionNumber': subNode.get('localExecutionNumber'),
            'providerNote': subNode.get('providerNote')
        }

    regex = re.compile(r'.*/subjects/(.+)')
    record_list = []
    for sampleId, subNode in subNode.items():
        # get linked values:
        links = {}
        if 'raw/wasExtractedFromAnatomicalRegion' in subNode:
            links['raw/wasExtractedFromAnatomicalRegion'] = subNode['raw/wasExtractedFromAnatomicalRegion']
        if 'wasDerivedFromSubject' in subNode:
            identifier = regex.match(subNode['wasDerivedFromSubject']).group(1)
            if identifier in recordCache['subject']:
                links['wasDerivedFromSubject'] = identifier

        record_list.append(transform(subNode))
        # rec = updateRecord(ds, recordCache, model, sampleId, transform(subNode), links)

        # if subNode.get('hasDigitalArtifactThatIsAboutIt') is not None:
        #     for fullFileName in subNode.get('hasDigitalArtifactThatIsAboutIt'):
        #         filename, file_extension = os.path.splitext(fullFileName)
        #         pkgs = ds.get_packages_by_filename(filename)
        #         if len(pkgs) > 0:
        #             for pkg in pkgs:
        #                 pkg.relate_to(rec)
    
    log.info('Creating {} new records'.format(len(record_list)))
    if len(record_list):
        recs = model.create_records(record_list)    

def addSummary(bf, ds, recordCache, identifier, subNode):
    log.info("Adding summary...")

    termModel = ds.get_model('term')
    model = getModel(bf, ds, 'summary', 'Summary', schema=[
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

    try:
        updateRecord(ds, recordCache, model, identifier, transform(subNode, description, isDescribedBy, hasExperimentalModality, hasResponsiblePrincipalInvestigator), relationships=relations, links=links)
    except Exception as e:
        log.error("Failed to add summary to dataset '{}'".format(ds))

def addAwards(bf, ds, recordCache, identifier, subNode):
    log.info("Adding awards...")

    model = getModel(bf, ds, 'award', 'Award', schema=[
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

    record_list = []
    for awardId, _ in subNode.items():
        record_list.append(transform(awardId))
        # updateRecord(ds, recordCache, model, awardId, transform(awardId))

    log.info('Creating {} new records'.format(len(record_list)))
    if len(record_list):
        recs = model.create_records(record_list)

def update_datasets(cfg, method = 'full'):
    """
    Update all datasets.
    if `reset`: clear and re-add all records. If not `reset`, only delete added items

    Returns: list of datasets that failed to update
    """

    log.info("Updating all datasets:")
    update_start_time = time()

    oldJson = {}
    if method == 'full':
        newJson = getJson('full')
    elif method == 'diff':
        oldJson, newJson = getJson('diff')
    else:
        raise(Exception('Inccorrect method: {}'.format(method)))

    failedDatasets = []
    if method == 'diff':
        log.info('===========================')
        log.info('== Deleting old metadata ==')
        log.info('===========================')
        log.info('')

        delete_start_time = time()

        ## Delete the old data in existing dataset for specific models
        for dsId, node in oldJson.items():
            log.info('Current dataset: {}'.format(dsId))

            # Get Dataset, or Create dataset with Name=dsId if it does not exist.
            ds = getCreateDataset(dsId)

            # If reset, then clear out all records. Otherwise, only clear out records that were 
            # added through this process
            if method == 'full':
                clearDataset(cfg.bf, ds)
                recordCache = {m: {} for m in MODEL_NAMES}
            else:
                recordCache = cfg.db_client.buildCache(dsId)

            models = {k: v for k, v in ds.models().items() if k in MODEL_NAMES}
            try:
                deleteData(ds, models, recordCache, node)
            except BlackfynnException:
                log.error("Dataset {} failed to update".format(dsId))
                failedDatasets.append(dsId)
                continue
            finally:
                cfg.db_client.writeCache(dsId, recordCache)

        duration = int((time() - delete_start_time) * 1000)
        log.info("Deleted old metadata in {} milliseconds".format(duration))

    log.info('===========================')
    log.info('=== Adding new metadata ===')
    log.info('===========================')
    log.info('')
    new_start_time = time()

    i = 0
    # Iterate over Datasets in JSON file and add metadata records...
    for dsId, node in newJson.items():

        if i==2:
            break
        i = i+1

        log.info("Creating records for dataset: {}".format(dsId))

        # Need to get existing dataset, or create new dataset (in dev)
        ds = getCreateDataset(cfg.bf, dsId)

        # Need to clear dataset records/models if full update and 
        # set cache
        if method == 'full':
            clearDataset(cfg.bf, ds)
            recordCache = {m: {} for m in MODEL_NAMES}
        else:
            recordCache = cfg.db_client.buildCache(dsId)

        # Add data from the JSON file to the BF Dataset
        try:
            addData(cfg.bf, ds, dsId, recordCache, node)
            addLinks(cfg.bf, ds, dsId, recordCache, node)
        except BlackfynnException:
            log.error("Dataset {} failed to update".format(dsId))
            failedDatasets.append(dsId)
            continue
        # finally:
            # cfg.db_client.writeCache(dsId, recordCache)

    # # Iterate over Datasets and add linked properties...
    # i = 0
    # for dsId, node in newJson.items():
    #     log.info("Create Links in records for dataset: {}".format(dsId))

    #     if i==2:
    #         break
    #     i = i+1

    #     # Need to get existing dataset
    #     ds = cfg.bf.get_dataset(dsId)

    #     # Add links from the JSON file to the appropriate records as they should exist
    #     addLinks(cfg.bf, ds, dsId, recordCache, node)
            





    # Timing stats
    duration = int((time() - new_start_time) * 1000)
    log.info("Added new metadata in {} milliseconds".format(duration))
    duration = int((time() - update_start_time) * 1000)
    log.info("Update datasets in {} milliseconds".format(duration))

    # Update dashboard when complete when running in production.
    if cfg.env == 'prod':
        update_sparc_dashboard()

    return 

    


# %%
