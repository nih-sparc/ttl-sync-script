from datetime import datetime as DT
from dateutil.parser import parse
import json
import logging
import re
import sys
import os
import requests
import math
import pyhash
from blackfynn.models import ModelPropertyEnumType, BaseCollection, ModelPropertyType
from blackfynn import Blackfynn, ModelProperty, LinkedModelProperty

from time import time
from bf_io import (
    authorized,
    get_create_dataset,
    clear_dataset,
    BlackfynnException,
    update_sparc_dashboard,
    get_create_model,
    get_create_hash_ds,
    clear_model,
    search_for_records,
    create_links,
    get_publication_status
)

from base import (
    JSON_METADATA_FULL,
    JSON_METADATA_NEW,
    SPARC_DATASET_ID,
    MODEL_NAMES,
    get_json,
    get_first,
    get_bf_model,
    get_as_list,
    parse_unit_value,
    has_bf_access,
    is_number,
    get_resume_list
)
from pprint import pprint

logging.basicConfig(format="%(asctime);s%(filename)s:%(lineno)d:\t%(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
fp = pyhash.farm_fingerprint_64()

### ENTRY POINT

def update_datasets(cfg, option = 'full', force_update = False, force_model = '', resume = False):
    """
    Update all datasets.

    Returns: list of datasets that failed to update
    """
    update_start_time = time()

    oldJson = {}
    newJson = get_json()

    updated_ds_list = []
    if resume:
        updated_ds_list = get_resume_list(cfg.ttl_resume_file )

    # If specific datasets is updated, select only current dataset
    if option != 'full':
        log.info("Updating dataset: {}".format(option))
        ds_info = newJson[option]
        newJson.clear() 
        newJson[option] = ds_info
    else:
        log.info("Updating all datasets:")

    failedDatasets = []

    # Get/create the synchronization dataset that captures the hash-identities per dataset
    sync_ds = get_create_hash_ds(cfg.bf)
    sync_rec_model = sync_ds.get_model('dataset')
    sync_recs = sync_rec_model.get_all(limit = 500)
    sync_dict = {x.get('ds_id'): x for x in sync_recs}

    # Iterate over Datasets in JSON file and add metadata records...
    for dsId, node in newJson.items():

        # Check if already updated in resume_list
        if dsId in updated_ds_list:
            log.info("--- Skipping due to resume: {} ---".format(dsId))
            continue            

        # Create a new file-logger for this dataset
        log_file_name = "/tmp/{}.log".format(dsId.replace(':','_'))

        if force_update:
            try:
                os.remove(log_file_name)
            except:
                pass

        filehandler = logging.FileHandler(log_file_name, 'a')
        for hdlr in log.handlers[:]:  # remove the existing file handlers
            if isinstance(hdlr,logging.FileHandler):
                log.removeHandler(hdlr)
        filehandler.setLevel(logging.INFO)
        log.addHandler(filehandler)

        log.warning('=== +++ ===')
        log.warning('--- {} ==='.format(dsId))
        log.warning('--- {} ---'.format(str(DT.now())))
        log.warning('=== +++ ===')

        # Create empty cache for records/models 
        record_cache = {m: {} for m in MODEL_NAMES}

        # Check if dataset exist in sync_dict
        if dsId in sync_dict:
            sync_rec = sync_dict[dsId]
        else:
            sync_rec = sync_rec_model.create_record({'ds_id': dsId})

        # Check which records should be updated
        update_recs = {}
        if not force_update:
            update_recs = {'protocol': get_recordset_hash(node['protocol']) != sync_rec.get('protocol'),
                'term': get_recordset_hash(node['term']) != sync_rec.get('term'),
                'researcher': get_recordset_hash(node['researcher']) != sync_rec.get('researcher'),
                'subject': get_recordset_hash(node['subject']) != sync_rec.get('subject'),
                'sample': get_recordset_hash(node['sample']) != sync_rec.get('sample'),
                'award': get_recordset_hash(node['award']) != sync_rec.get('award'),
                'summary': get_recordset_hash(node['summary']) != sync_rec.get('summary'),
                'tag': get_recordset_hash(node['tag']) != sync_rec.get('tag')}  
        else:
              update_recs = {'protocol': True,
                'term': True,
                'researcher': True,
                'subject': True,
                'sample': True,
                'award': True,
                'summary': True,
                'tag': True} 
        
        # If force model is set, then always update provided model
        if force_model:
            update_recs[force_model] = True

        # Add data from the JSON file to the BF Dataset
        try:
            if any([ update_recs[x] for x in update_recs.keys()]):

                # Need to get existing dataset, or create new dataset (in dev)
                ds = get_create_dataset(cfg.bf, dsId)

                # Check that curation bot has manager access
                if cfg.env=='prod' and not has_bf_access(ds):
                    log.warning('UNABLE TO UPDATE DATASET DUE TO PERMISSIONS: {}'.format(dsId))
                    continue

                # Check if dataset is locked
                publication_status = get_publication_status(cfg.bf, dsId)
                log.info('PUBLICATION STATUS: {}'.format(publication_status))
                if publication_status in ['accepted', 'requested', 'failed']:
                    log.warning('UNABLE TO UPDATE DATASET (status: {}) : {}'.format(publication_status, dsId))
                    continue
                

                # Create all records
                add_data(cfg.bf, ds, dsId, record_cache, node, sync_rec, update_recs)

                # Create all links between records
                add_links(cfg.bf, ds, dsId, record_cache, node, update_recs)

                # Add Dataset tag
                add_tags(cfg.bf, ds, node['tag'], sync_rec, update_recs)

                # Update Sync Records 
                log.info('UPDATING SYNC RECORD')
                sync_rec.update()
            else:
                log.info('=== No Records changed, skipping dataset ===')

        except BlackfynnException:
            log.error("Dataset {} failed to update".format(dsId))            
            failedDatasets.append(dsId)
            continue

        updated_ds_list.append(dsId)    
        with open(cfg.ttl_resume_file , 'w') as f:
            json.dump(updated_ds_list, f)

        log.info('===========================')

    # Timing stats
    duration = int((time() - update_start_time) * 1000)
    log.info("Update datasets in {} milliseconds".format(duration))

    # Update dashboard when complete when running in production.
    if cfg.env == 'prod':
        update_sparc_dashboard(cfg.bf)

    return 

### CORE METHODS

def map_target_to_json_model(target_name):
    """Maps between platform model name and JSON model identifier
    """

    if target_name == 'protocol':
        return 'protocol'
    elif target_name == 'summary':
        return 'summary'
    elif target_name == 'subject':
        return 'subject'
    elif target_name == 'animal_subject':
        return 'subject'
    elif target_name == 'human_subject':
        return 'subject'
    elif target_name == 'tag':
        return 'tag'
    elif target_name == 'sample':
        return 'sample'
    elif target_name == 'researcher':
        return 'researcher'
    elif target_name == 'award':
        return 'award'
    elif target_name == 'term':
        return 'term'
    
def get_record_id_from_node(bf, ds, model, json_id, json_node, record_cache):

    if json_id in record_cache[model.type]:
        record = record_cache[model.type][json_id]
        return record.id
    else:
        result = find_target_record(bf, ds, model.type, json_node, json_id)
        if result:
            record_cache[model.type][json_id] = model.get(result['id'])
            return result['id']
        else:
            log.debug('Cannot find item in cache or on Platform: {}'.format(json_id))
            return None

def find_target_record(bf, ds, target_type, json_node, json_id):
    """Search for record on platform based on JSON identity

    Because the JSON ID is not always stored in the platform, we need to find the record
    by performing a search

    Returns JSON representation of record
    """

    if target_type == 'award':
        # Award can be identified by 
        record_filter = [{
            "model":target_type,
            "property":"award_id",
            "operator":"=",
            "value":json_node['awardId']}]
    elif target_type == 'sample':
        record_filter = [{
            "model":target_type,
            "property":"id",
            "operator":"=",
            "value":json_id}]
    elif target_type == 'term':
        if json_node:
            record_filter = [{
                "model":target_type,
                "property":"label",
                "operator":"=",
                "value":get_first(json_node, 'labels', '(no label)')}]
        else:
            record_filter = [{
                "model":target_type,
                "property":"label",
                "operator":"=",
                "value":json_id}]
    elif target_type == 'researcher':
        record_filter = [{
            "model":target_type,
            "property":"lastName",
            "operator":"=",
            "value":json_node.get('lastName', '(no label)')},
            {
            "model":target_type,
            "property":"firstName",
            "operator":"=",
            "value":json_node.get('firstName')}]    
    elif target_type == 'summary':
        record_filter = []    
    else:
        return None
        
    log.debug("Searching for node with filter:  {} - {}".format(target_type, record_filter))

    out = search_for_records(bf, ds, target_type, record_filter)

    log.debug("Result of search: {}".format(out))

    return out

def update_records(bf, ds, sub_node, model_name, record_cache, model_create_fnc, transform_fnc):
    """Creates records for particular Model in Dataset

    This method takes the sub_node for a particular model in a dataset and create the records.
    
    Parameters
    ----------
    bf: Blackfynn
        Blackfynn session
    ds: BF_Dataset
        Dataset that contains the records
    sub_node: Dict
        JSON sub_node for specific model in specific dataset
    model_name: str
        Name of the current model
    recordCache: Dict
        Map of all ids to records in current dataset
    model_create_fnc: function()
        Function to create model of type "model_name"
    transfors_fnc: function()
        Function to transform JSON node to record property/value pairs

    """
    record_list = []
    json_id_list = []
    for record_id, sub_node in sub_node.items():
        record_list.append(transform_fnc(record_id, sub_node))
        json_id_list.append("{}".format( record_id ))

    model = model_create_fnc(bf, ds)
    if len(record_list):
        log.info('Creating {} new {} Records'.format(len(record_list), model_name))

        # Add batches of max 100 records
        n = 100
        nr_batches = math.floor(len(record_list) /n )
        if nr_batches > 1:
            for i in range(0, nr_batches):
                record_cache[model_name].update(zip(json_id_list[i*n:(i*n+n)], model.create_records(record_list[i*n:(i*n+n)])))

            record_cache[model_name].update(zip(json_id_list[(i+1)*n:], model.create_records(record_list[(i+1)*n:])))
        else:
            record_cache[model_name].update(zip(json_id_list, model.create_records(record_list)))
            
        log.debug('Finished creating records')

    else:
        log.info('No records to be created')

def get_recordset_hash(node):
    """Return hash of current json node

    This method is used to represent a state of record set within the dataset. If the hash between 
    the new json file is different from the one associated with what is on the platfom, some of the records
    have been altered. 
    """
    return str(fp(json.dumps(node, sort_keys=True)))
    
def add_data(bf, ds, dsId, record_cache, node, sync_rec, update_recs):
    """Iterate over specific models and add records

    This method is called as the core method to add records to datasets.

     Parameters
    ----------
    bf: Blackfynn
        Blackfynn session
    ds: BF_Dataset
        Dataset that contains the records
    dsId: str
        Dataset ID
    recordCache: Dict
        Map of all ids to records in current dataset
    node: Dict
        JSON sub_node for dataset
    sync_rec: Dict
        Dict with hash values of each record set per model that is synced

    """

    # Get Models
    models = ds.models()


    # Adding all records without setting linked properties and relationships
    if update_recs['protocol']:
        log.info('Updating protocol')
        clear_model(bf, ds, 'protocol')
        add_protocols(bf, ds, record_cache, node['protocol'])
        sync_rec._set_value('protocol', get_recordset_hash(node['protocol']))
    else:
        log.info('Skipping protocol')
    
    if update_recs['term']:
        log.info('Updating term')
        clear_model(bf, ds, 'term')
        add_terms(bf, ds, record_cache, node['term'])
        sync_rec._set_value('term', get_recordset_hash(node['term']))
    else:
        log.info('Skipping term')

    if update_recs['researcher']:
        log.info('Updating researcher')
        clear_model(bf, ds, 'researcher')
        add_researchers(bf, ds, record_cache, node['researcher'])
        sync_rec._set_value('researcher', get_recordset_hash(node['researcher']))
    else:
        log.info('Skipping researcher')

    if update_recs['subject']:
        log.info('Updating subject')
        clear_model(bf, ds, 'animal_subject')
        clear_model(bf, ds, 'human_subject')
        add_subjects(bf, ds, record_cache, node['subject'])
        sync_rec._set_value('subject', get_recordset_hash(node['subject']))
    else:
        log.info('Skipping subject')
    
    if update_recs['sample']:
        log.info('Updating sample')
        clear_model(bf, ds, 'sample')
        add_samples(bf, ds, record_cache, node['sample'])
        sync_rec._set_value('sample', get_recordset_hash(node['sample']))
    else:
        log.info('Skipping sample')

    if update_recs['award']:
        log.info('Updating award')
        clear_model(bf, ds, 'award')
        add_awards(bf, ds, record_cache, node['award'])
        sync_rec._set_value('award', get_recordset_hash(node['award']))
    else:
        log.info('Skipping award')

    if update_recs['summary']:
        log.info('Updating summary')
        clear_model(bf, ds, 'summary')
        add_summary(bf, ds, record_cache, node['summary'])
        sync_rec._set_value('summary', get_recordset_hash(node['summary']))
    else:
        log.info('Skipping summary')

def add_links(bf, ds, dsId, record_cache, node, update_recs):
    """Iterate over specific models and add property links and relationships

    This method is called as the core method to add property links and relationships to records.

     Parameters
    ----------
    bf: Blackfynn
        Blackfynn session
    ds: BF_Dataset
        Dataset that contains the records
    dsId: str
        Dataset ID
    recordCache: Dict
        Map of all ids to records in current dataset
    node: Dict
        JSON sub_node for dataset

    """

    # Adding all linked properties and relationships to records
    if update_recs['summary'] or update_recs['term'] or update_recs['award'] or update_recs['researcher']:
        log.info('Adding links to summary record')
        add_summary_links(bf, ds, record_cache, 'summary', node)

    if update_recs['subject'] or update_recs['term'] :
        log.info('Adding links to subject record')
        add_subject_links(bf, ds, record_cache, 'subject', node)

    if update_recs['sample'] or update_recs['term'] or update_recs['subject']: 
        log.info('Adding links to sample record')
        add_sample_links(bf,ds, record_cache, 'sample', node)

def add_random_terms(ds, label, record_cache):
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

    if not hasattr(add_random_terms, "term_model"):
        add_random_terms.term_model = get_bf_model(ds, 'term')
        add_random_terms.model_ds = ds.id
    elif add_random_terms.model_ds != ds.id:
        add_random_terms.term_model = get_bf_model(ds, 'term')
        add_random_terms.model_ds = ds.id
 
    log.debug("Adding random term: {}".format(label))

    record = add_random_terms.term_model.create_record({'label': label})
    record_cache['term'][label] = record
    return record

def add_record_links(bf, ds, record_cache, model, record_id, links, ds_node):
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
    record_id: String
        ID of Record that is being updated
    links: Array [ {name:  Node }]
        linked values (structured {name: identifier})
    bf: Blackfynn Session
    ds_node: Dict
        Dict from JSON with current dataset objects (for lookup)

    """

    log.debug('Adding Record Linked Properties for {}-{}'.format(model, record_id))
    payload =  []
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

        # terms = None
        linkedProp = model.linked[name]

        # Find model name of the linked property target
        target_model = get_bf_model(ds, linkedProp.target)
        targetType = target_model.type

        # We can have an array of links per property 
        linked_rec_id = None
        for json_id in valueList:
            # Check if value is in the record cache
            json_model_name = map_target_to_json_model(targetType)    

            item_node = []
            if json_id in ds_node[json_model_name]:
                item_node =  ds_node[json_model_name][json_id]
            
            # Find item in cache or platform
            linked_rec_id = get_record_id_from_node(bf, ds, target_model, json_id, item_node, record_cache )

            if not linked_rec_id: 
                if targetType == 'term':
                    linked_rec = add_random_terms(ds, json_id, record_cache)
                    linked_rec_id = linked_rec.id
                else:
                    log.warning('UNABLE to LINK ({}:{}) to non-existing record ({}:{})'.format(model.type, record_id, targetType, json_id))

            if linked_rec_id:
                payload.append({
                    "name": targetType,
                    "schemaLinkedPropertyId" : linkedProp.id,
                    "to": linked_rec_id    
                })
        
    log.debug("Updating Linked Properties: {} : record ID: {}".format(payload, record_id))   
    if len(payload): 
        create_links(bf, ds, model.id, record_id, payload)
        
def add_record_relationships(bf, ds, record_cache, model, record, relationships, ds_node):
    
    log.debug('Adding Record Relationships for {}'.format(record.id))
    # Iterate over all relationships in a record
    for name, value in relationships.items():
        targetRecordList = []

        targetModel = value['type']

        target_model_instance = get_bf_model(ds, targetModel)
        value = value['node']

        valueList = None
        if isinstance(value, str):
            valueList = [value]
        elif isinstance(value, list):
            valueList = value
        elif value == None:
            continue
        else:
            raise(Exception('Incorrect type for relationship node.'))

        # Iterate over all items with particular relationship to record
        for json_id in valueList:

            # Because json-model name can be different than Platform model name (e.g. Subject vs Animal_Subject)
            json_model_name = map_target_to_json_model(targetModel)    

            item_node = []
            if json_id in ds_node[json_model_name]:
                item_node =  ds_node[json_model_name][json_id]

                # Find item in cache or platform
                linked_rec_id = get_record_id_from_node(bf, ds, target_model_instance, json_id, item_node, record_cache )

                if linked_rec_id:
                    targetRecordList.append(target_model_instance.get(linked_rec_id))
                elif targetModel == 'term':
                    log.debug("Adding a string term to the dataset: {}".format(json_id))
                    linked_rec = add_random_terms(ds, json_id, record_cache)
                    targetRecordList.append(linked_rec)
                else:
                    log.warning('UNABLE to RELATE record ({}) to non-existing record {}:{}'.format(record.id, targetModel, json_id))

        # Add to list
        if len(targetRecordList) > 0:
            record.relate_to(targetRecordList, name)

def add_tags(bf, ds, sub_node, sync_rec, update_recs):
    """Adding Dataset Tags based on the Tags defined in the TTL file

    Parameters
    ----------
    ds: BF_Dataset
        Dataset that contains the records
    sub_node: [String]
        Representation of tags in JSON file
    bf: Blackfynn Session
    """

    if update_recs['tag']:
        log.info("Adding tag...")

        tags = sub_node
        if not tags:
            tags = ['SPARC']
            
        ds.tags = list(set(tags))
        ds.update()

        sync_rec._set_value('tag', get_recordset_hash(sub_node))
    else:
        print('Skipping tag')

def get_unit_map(sub_node):
    """Get dict with unit for property

    {unit: x, class: str}
    """

    out = {}
    for item, val_dict in sub_node.items():
        for key, value in val_dict.items():
            if isinstance(value, dict):
                if 'unit' in value:
                    if key in out:
                        if value['unit'] != out[key]['unit'] and out[key]['unit'] != '(no unit)':
                            log.warning("Multiple units for model-property in single dataset: {} and {}".format(value['unit'], out[key]['unit']))
                        if out[key]['is_num'] != is_number(value['value']):
                            log.warning("Not all values are parseable as floats: {}".format(value['value']))
                            out[key]['is_num'] = False
                    else:
                        if value['unit']:
                            is_num = is_number(value['value'])
                            out[key] = { 
                                'unit': value['unit'], 
                                'is_num': is_num}
                        else:
                            is_num = is_number(value['value'])
                            out[key] = { 
                                'unit': '(no unit)', 
                                'is_num': is_num}
    
    return out


### MODEL SPECIFIC METHODS

def add_protocols(bf, ds, record_cache, sub_node):
    log.info("Adding protocols...")

    def create_model(bf, ds):
        return get_create_model(bf, ds, 'protocol', 'Protocol', schema=[
            ModelProperty('label', 'Name', title=True),
            ModelProperty('url', 'URL',data_type=ModelPropertyType(
                    data_type=str, format='url')),
            ModelProperty('publisher', 'publisher'),
            ModelProperty('date', 'Date', data_type=ModelPropertyType(
                    data_type='date' )),
            ModelProperty('protocolHasNumberOfSteps', 'Number of Steps'), 
            ModelProperty('hasNumberOfProtcurAnnotations', 'Number of Protcur Annotations')
        ])

    def transform(record_id, sub_node):
        return {
             'label': sub_node.get('label', '(no label)'),
             'url': record_id, #sub_node.get('http://www.w3.org/2002/07/owl#sameAs'),
             'date': sub_node.get('date'),
             'publisher': sub_node.get('publisher'),
             'protocolHasNumberOfSteps': sub_node.get('protocolHasNumberOfSteps'),
             'hasNumberOfProtcurAnnotations': sub_node.get('hasNumberOfProtcurAnnotations')
        }

    update_records(bf, ds, sub_node, "protocol", record_cache,  create_model, transform)

def add_terms(bf, ds, record_cache, sub_node):

    def create_model(bf, ds):
        return get_create_model(bf, ds, 'term', 'Term', schema=[
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
        
    def transform(record_id, term):
        return {
            'label': get_first(term, 'labels', '(no label)'),
            'curie': term.get('curie'),
            'definitions': get_first(term, 'definitions'),
            'abbreviations': term.get('abbreviations'),
            'synonyms': term.get('synonyms'),
            'acronyms': term.get('acronyms'),
            'categories': term.get('categories'),
            'iri': term.get('iri'),
        }

    update_records(bf, ds, sub_node, "term", record_cache,  create_model, transform)

def add_researchers(bf, ds, record_cache, sub_node):

    def create_model(bf, ds):
        return get_create_model(bf, ds, 'researcher', 'Researcher', schema=[
                ModelProperty('lastName', 'Last name', title=True),
                ModelProperty('firstName', 'First name'),
                ModelProperty('middleName', 'Middle name'),
                ModelProperty('hasAffiliation', 'Affiliation', data_type=ModelPropertyEnumType(
                    data_type=str, multi_select=True)), # list
                ModelProperty('hasRole', 'Role', data_type=ModelPropertyEnumType(
                    data_type=str, multi_select=True)), # list
                ModelProperty('hasORCIDId', 'ORCID iD', data_type=ModelPropertyType(
                    data_type=str, format='url'))
        ])

    def transform(record_id, sub_node):
        return {
            'lastName': sub_node.get('lastName', '(no label)'),
            'firstName': sub_node.get('firstName'),
            'middleName': sub_node.get('middleName'),
            'hasAffiliation': sub_node.get('hasAffiliation'),
            'hasRole': sub_node.get('hasRole'),
            'hasORCIDId': sub_node.get('hasORCIDId')
        }

    update_records(bf,ds,sub_node, "researcher", record_cache,  create_model, transform)

def add_subjects(bf, ds, record_cache, sub_node):
    term_model = get_bf_model(ds, 'term')

    ## Get Model-unit map for dataset
    unit_map = get_unit_map(sub_node)

    # Set defaults if not defined
    if 'hasAge' not in unit_map:
        unit_map['hasAge'] = {'unit': None, 'is_num': False }

    ## Define Model Generators
    def create_human_model(bf, ds, unit_map):
        return get_create_model(bf, ds, 'human_subject', 'Human Subject',
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
                LinkedModelProperty('hasBiologicalSex', term_model, 'Biological sex'), # list (this is a bug)
                LinkedModelProperty('hasAgeCategory', term_model, 'Age category'),
                LinkedModelProperty('specimenHasIdentifier', term_model, 'Identifier'),
            ]
            )

    def create_animal_model(bf, ds, unit_map):

        # Create ModelProperties
        if unit_map['hasAge']['is_num']:
            has_age_model_prop = ModelProperty('hasAge', 'Age',data_type=ModelPropertyType(
                data_type=float, unit=unit_map['hasAge']['unit'] )) # unit+value
        else:
            has_age_model_prop = ModelProperty('hasAge', 'Age',data_type=ModelPropertyType(
                data_type=str )) # unit+value


        return get_create_model(bf, ds, 'animal_subject', 'Animal Subject',
            schema=[
                ModelProperty('localId', 'Subject ID', title=True),
                ModelProperty('animalSubjectIsOfStrain', 'Animal strain'),
                ModelProperty('animalSubjectHasWeight', 'Animal weight'), # unit+value
                has_age_model_prop,
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
                LinkedModelProperty('animalSubjectIsOfSpecies', term_model, 'Animal species'),
                # LinkedModelProperty('animalSubjectIsOfStrain', term_model, 'Animal strain'),
                LinkedModelProperty('hasBiologicalSex', term_model, 'Biological sex'), # list (this is a bug)
                LinkedModelProperty('hasAgeCategory', term_model, 'Age category'),
                LinkedModelProperty('specimenHasIdentifier', term_model, 'Identifier'),
            ])

    ## Define Transform methods
    def transform_human(sub_node, local_id, unit_map):
        vals = {
            'localId': local_id,
            'localExecutionNumber': sub_node.get('localExecutionNumber'),
            'subjectHasWeight': parse_unit_value(sub_node, 'subjectHasWeight', 'kg'),
            'subjectHasHeight': parse_unit_value(sub_node, 'subjectHasHeight'),
            'hasAge': parse_unit_value(sub_node, 'hasAge', unit_map['hasAge']['unit'],unit_map['hasAge']['is_num']),
            'spatialLocationOfModulator': sub_node.get('spatialLocationOfModulator'),
            'stimulatorUtilized': sub_node.get('stimulatorUtilized'),
            'hasAssignedGroup': sub_node.get('hasAssignedGroup'),
            'providerNote': sub_node.get('providerNote'),
            'raw/involvesAnatomicalRegion': sub_node.get('raw/involvesAnatomicalRegion'),
            'hasGenotype': sub_node.get('hasGenotype'),
            'wasAdministeredAnesthesia': sub_node.get('wasAdministeredAnesthesia')
        }

        return vals
    
    def transform_animal(sub_node, local_id, unit_map):
        vals = {
            'localId': local_id,
            'localExecutionNumber': sub_node.get('localExecutionNumber'),
            'hasAge': parse_unit_value(sub_node, 'hasAge', unit_map['hasAge']['unit'],unit_map['hasAge']['is_num']),
            'spatialLocationOfModulator': sub_node.get('spatialLocationOfModulator'),
            'stimulatorUtilized': sub_node.get('stimulatorUtilized'),
            'hasAssignedGroup': sub_node.get('hasAssignedGroup'),
            'providerNote': sub_node.get('providerNote'),
            'raw/involvesAnatomicalRegion': sub_node.get('raw/involvesAnatomicalRegion'),
            'hasGenotype': sub_node.get('hasGenotype'),
            'animalSubjectIsOfStrain': sub_node.get('animalSubjectIsOfStrain'),
            'animalSubjectHasWeight': parse_unit_value(sub_node, 'animalSubjectHasWeight'),
            'wasAdministeredAnesthesia': sub_node.get('wasAdministeredAnesthesia')
        }

        try:
            vals['protocolExecutionDate'] = [DT.strptime(x, '%m-%d-%y') for x in sub_node['protocolExecutionDate']]
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
    for subj_id, subj_node in sub_node.items():
        subtype = sub_node.get('animalSubjectIsOfSpecies')
        if subtype == 'homo sapiens':
            human_record_list.append(transform_human(subj_node, subj_id, unit_map))
            human_json_id_list.append("{}".format(subj_id))
        else:
            animal_record_list.append(transform_animal(subj_node, subj_id, unit_map))
            animal_json_id_list.append("{}".format( subj_id))
    
    ## Create records
    if len(human_record_list) > 0:
        log.info('Creating {} new human_subject Records'.format(len(human_record_list)))
        human_model = create_human_model(bf, ds, unit_map)
        record_cache['human_subject'].update(zip(human_json_id_list,human_model.create_records(human_record_list)))

    elif len(animal_record_list) > 0:
        log.info('Creating {} new animal_subject Records'.format(len(animal_record_list)))
        animal_model = create_animal_model(bf, ds, unit_map)
        record_cache['animal_subject'].update(zip(animal_json_id_list,animal_model.create_records(animal_record_list)))

def add_subject_links(bf, ds, record_cache, sub_node_name, ds_node): 

    sub_node = ds_node[sub_node_name]
    model = None

    subtype = sub_node.get('animalSubjectIsOfSpecies')
    try:
        if subtype == 'homo sapiens':
            model = get_bf_model(ds, 'human_subject')
        else:
            model = get_bf_model(ds, 'animal_subject')
    except:
        # No models for subject defined
        return


    def transform_human(sub_node, localId):
        links = {
            'hasBiologicalSex': sub_node.get('hasBiologicalSex'),
            'hasAgeCategory': sub_node.get('hasAgeCategory'),
            'specimenHasIdentifier':sub_node.get('specimenHasIdentifier')
        }
        return links

    def transform_animal(sub_node, localId):
        links = {
            'animalSubjectIsOfSpecies': sub_node.get('animalSubjectIsOfSpecies'),
            'hasBiologicalSex': sub_node.get('hasBiologicalSex'),
            'hasAgeCategory': sub_node.get('hasAgeCategory'),
            'specimenHasIdentifier':sub_node.get('specimenHasIdentifier')
        }
        return links

    # Iterate over multiple subject records, single dataset
    for subj_id, subj_node in sub_node.items():
        record_id = get_record_id_from_node(bf, ds, model, subj_id, sub_node, record_cache)

        if record_id:
            if subtype == 'homo sapiens':
                links = transform_human(subj_node, subj_id)
            else:
                links = transform_animal(subj_node, subj_id)

            add_record_links(bf, ds, record_cache, model, record_id, links, ds_node)
        else:
            log.warning('Trying to link to a subject record ({}) that does not exist.'.format(record_id  ))

def add_samples(bf, ds, record_cache, sub_node):

    def create_sample_model(bf, ds):

        return get_create_model(bf, ds, 'sample', 'Sample',
            schema=[
                ModelProperty('label', 'Label', title=True),
                ModelProperty('id', 'id'),
                ModelProperty('description', 'Description'), # list
                ModelProperty('hasAssignedGroup', 'Group', data_type=ModelPropertyEnumType(
                    data_type=str, multi_select=True)), # list
                ModelProperty('extractedFrom', 'Extract Location', data_type=ModelPropertyEnumType(
                    data_type=str, multi_select=True)), # filename list
                ModelProperty('hasDigitalArtifactThatIsAboutIt', 'Digital artifact', data_type=ModelPropertyEnumType(
                    data_type=str, multi_select=True)), # filename list
                #ModelProperty('hasDigitalArtifactThatIsAboutItHash', ), # list
                ModelProperty('localExecutionNumber', 'Execution number', data_type=ModelPropertyEnumType(
                    data_type=str, multi_select=True)), # list
                ModelProperty('providerNote', 'Provider note', data_type=ModelPropertyEnumType(
                    data_type=str, multi_select=True)), # list
            ])

    def transform(record_id, sub_node):
        return {
            'id': record_id,
            'description': get_first(sub_node, 'description'),
            'hasAssignedGroup': sub_node.get('hasAssignedGroup'),
            'hasDigitalArtifactThatIsAboutIt': sub_node.get('hasDigitalArtifactThatIsAboutIt'),
            'extractedFrom':sub_node.get('raw/wasExtractedFromAnatomicalRegion'),
            'label': sub_node.get('localId'),
            'localExecutionNumber': sub_node.get('localExecutionNumber'),
            'providerNote': sub_node.get('providerNote')
        }

    update_records(bf,ds,sub_node, "sample", record_cache,  create_sample_model, transform)

def add_sample_links(bf, ds, record_cache, sub_node_name, ds_node):

    sub_node = ds_node[sub_node_name]

    # Skip if Model is not defined.
    if get_bf_model(ds, 'sample') is None:
        return

    def updateModel(bf, ds):
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
            clear_model(bf, ds, 'subject')
            subModel = get_create_model(bf, ds, 'subject', 'Subject',
                schema=[
                    ModelProperty('localId', 'ID', title=True)
                ]
                )
    
        return get_create_model(bf, ds, 'sample', 'Sample', linked=[
                # LinkedModelProperty('extractedFromAnatomicalRegion', get_bf_model(ds, 'term'), 'Extracted from anatomical region'),
                LinkedModelProperty('wasDerivedFromSubject', subModel, 'Derived from subject')
            ])

    def transform_sample(sub_node):
        subj_id = None
        if 'wasDerivedFromSubject' in sub_node:
            regex = re.compile(r'.*/subjects/(.+)')
            subj_id = regex.match(sub_node['wasDerivedFromSubject']).group(1)

        links = {
            'wasDerivedFromSubject': subj_id,
        }

        relationships = {
            # 'extracted_from_anatomical_region': {'type': 'term', 'node': sub_node.get('raw/wasExtractedFromAnatomicalRegion')},
        }
        
        return {
            'links':links, 
            'relationships':relationships}

    # Add Property links to model
    model = updateModel(bf, ds)

    # Iterate over multiple subject records, single dataset
    for sampleId, subj_node in sub_node.items():
        record_id = get_record_id_from_node(bf, ds, model, sampleId, subj_node, record_cache)
        
        if record_id:
            out = transform_sample(subj_node)

            # Adding Linked Properties
            add_record_links(bf, ds, record_cache, model, record_id, out['links'], ds_node)
        
            # Adding Relationships
            record = model.get(record_id) #TODO: Remove this
            rels = out['relationships']
            add_record_relationships(bf, ds, record_cache, model, record, out['relationships'], ds_node)

            # Associate files with Samples
            if sub_node.get('hasDigitalArtifactThatIsAboutIt') is not None:
                for fullFileName in sub_node.get('hasDigitalArtifactThatIsAboutIt'):
                    log.info('Adding File Links: {}'.format(fullFileName))
                    filename, file_extension = os.path.splitext(fullFileName)
                    pkgs = ds.get_packages_by_filename(filename)
                    if len(pkgs) > 0:
                        for pkg in pkgs:
                            pkg.relate_to(record)
            
        else:
            log.warning('Trying to link to a sample record ({}) that does not exist.'.format( record_id ))
                    
def add_summary(bf, ds, record_cache, sub_node):
    log.info("Adding summary...")
    
    def create_model(bf, ds):
        return get_create_model(bf, ds, 'summary', 'Summary', schema=[
            ModelProperty('title', 'Title', title=True), # list
            # ModelProperty('hasResponsiblePrincipalInvestigator', 'Responsible Principal Investigator',
            #             data_type=ModelPropertyEnumType(data_type=str, multi_select=True)),
            # list of ORCID URLs, blackfynn user IDs, and, and Blackfynn contributor URLs
            # TODO: make this a relationship?
            ModelProperty('isDescribedBy', 'Publication URL', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # list (of urls)
            ModelProperty('description', 'Description', data_type=ModelPropertyEnumType(
                data_type=str, multi_select=True)), # list
            # TODO: update dataset description using PUT /datasets/{id}/readme
            ModelProperty('collectionTitle', 'Collection Title'),
            ModelProperty('milestoneCompletionDate', 'Milestone Completion Date', data_type=ModelPropertyType(
                    data_type='date' )),
            ModelProperty('curationIndex', 'Curation index'), # number string
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
            LinkedModelProperty('hasAwardNumber', get_bf_model(ds, 'award'), 'Award number'),
            
        ])

    def transform(record_id, sub_node):
        # Check Milestone Completion Data is a date:
        try:
            milestoneDate = parse(sub_node.get('milestoneCompletionDate'))
            try:
                milestoneDate = milestoneDate.isoformat()
            except:
                log.warning('Cannot parse the Milestone Date: {}'.format(sub_node.get('milestoneCompletionDate')))
                milestoneDate = None
        except:
            milestoneDate = None

        return {
            'milestoneCompletionDate': milestoneDate,
            'isDescribedBy': get_as_list(sub_node, 'isDescribedBy'),
            'acknowledgements': sub_node.get('acknowledgements'),
            'collectionTitle': sub_node.get('collectionTitle'),
            'curationIndex': sub_node.get('curationIndex'),
            'description': get_as_list(sub_node, 'description'),
            'errorIndex': sub_node.get('errorIndex'),
            'hasExperimentalModality': get_as_list(sub_node, 'hasExperimentalModality'),
            'hasNumberOfContributors': sub_node.get('hasNumberOfContributors'),
            'hasNumberOfDirectories': sub_node.get('hasNumberOfDirectories'),
            'hasNumberOfFiles': sub_node.get('hasNumberOfFiles'),
            'hasNumberOfSamples': sub_node.get('hasNumberOfSamples'),
            'hasNumberOfSubjects': sub_node.get('hasNumberOfSubjects'),
            'hasSizeInBytes': sub_node.get('hasSizeInBytes'),
            'label': sub_node.get('label'),
            'submissionIndex': sub_node.get('submissionIndex'),
            'title': sub_node.get('title')
        }

    record_list = []
    json_id_list = []
    
    # No iteration because there is only one summary.
    record_list.append(transform('summary', sub_node))
    json_id_list.append("{}".format( 'summary' ))

    if len(record_list):
        log.info('Creating {} new summary Records'.format(len(record_list)))
        model = create_model(bf, ds)
        record_cache['summary'].update(zip(json_id_list, model.create_records(record_list)))

def add_summary_links(bf, ds, record_cache, sub_node_name, ds_node):

    sub_node = ds_node[sub_node_name]
    model = get_bf_model(ds, 'summary')

    def updateModel(bf, ds):
        return get_create_model(bf, ds, 'summary', 'Summary', linked=[
                LinkedModelProperty('hasAwardNumber', get_bf_model(ds, 'award'), 'Award number')
            ])

    def transform(sub_node):
        links = {
            'hasAwardNumber': sub_node.get('hasAwardNumber'),
        }
        relationships = {
            'hasResponsiblePrincipalInvestigator': {'type': 'researcher', 'node': sub_node.get('hasResponsiblePrincipalInvestigator')},
            'hasContactPerson': {'type': 'researcher', 'node': sub_node.get('hasContactPerson')},
            'involvesAnatomicalRegion': {'type': 'term', 'node': sub_node.get('involvesAnatomicalRegion')},
            'protocolEmploysTechnique': {'type': 'term', 'node': sub_node.get('protocolEmploysTechnique')},
            'isAbout': {'type': 'term', 'node': sub_node.get('http://purl.obolibrary.org/obo/IAO_0000136')}

        }
        return {
            'links':links, 
            'relationships':relationships}
    
    # Add Property links to model
    model = updateModel(bf, ds)

    record_id = get_record_id_from_node(bf, ds, model, 'summary', sub_node, record_cache  ) 
    
    if record_id:
        # Add Linked Properties
        out = transform(sub_node)
        add_record_links(bf, ds, record_cache, model, record_id, out['links'], ds_node )

        # Add Relationships
        rels = out['relationships']
        record = model.get(record_id) #TODO update to use ID only
        add_record_relationships(bf, ds, record_cache, model, record, out['relationships'], ds_node)
    else:
        log.warning('Trying to link to a summary record ({}) that does not exist.'.format( record_id ))

def add_awards(bf, ds, record_cache, sub_node):

    def create_model(bf, ds):
        return get_create_model(bf, ds, 'award', 'Award', schema=[
            ModelProperty('award_id', 'Award ID', title=True),
            ModelProperty('title', 'Title'),
            ModelProperty('description', 'Description'),
            ModelProperty('principal_investigator', 'Principal Investigator'),

        ])

    def transform(record_id, sub_node):
        awardId = sub_node.get('awardId')
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

    update_records(bf, ds, sub_node, "award", record_cache,  create_model, transform)