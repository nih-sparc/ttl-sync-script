from datetime import datetime as DT
from dateutil.parser import parse
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
    get_create_dataset,
    clear_dataset,
    BlackfynnException,
    update_sparc_dashboard,
    get_create_model
)

from base import (
    JSON_METADATA_FULL,
    JSON_METADATA_NEW,
    SPARC_DATASET_ID,
    MODEL_NAMES,
    get_record_by_id,
    get_json,
    get_first,
    get_bf_model,
    get_as_list,
    parse_unit_value,
    has_bf_access
)
from pprint import pprint

logging.basicConfig(format="%(asctime);s%(filename)s:%(lineno)d:\t%(message)s")
log = logging.getLogger(__name__)

### ENTRY POINT

def update_datasets(cfg, option = 'full', resume=None):
    """
    Update all datasets.
    if `reset`: clear and re-add all records. If not `reset`, only delete added items

    Returns: list of datasets that failed to update
    """

    log.info("Updating all datasets:")
    update_start_time = time()

    oldJson = {}
    newJson = get_json('full')

    if option != 'full':
        # Get specific dataset from JSON
        ds_info = newJson[option]
        newJson.clear() 
        newJson[option] = ds_info

    failedDatasets = []
       
    log.info('===========================')
    log.info('=== Adding new metadata ===')
    log.info('===========================')
    log.info('')
    new_start_time = time()

    is_resuming = True

    log.info('RESUME = {}'.format(resume))

    # Iterate over Datasets in JSON file and add metadata records...
    for dsId, node in newJson.items():

        # Skip datasets until resume dataset is found if it exists
        if resume and is_resuming:
            if dsId != resume:
                log.info('Skipping dataset: {}'.format(dsId))
                continue
            else:
                is_resuming = False


        log.info("Creating records for dataset: {}".format(dsId))

        # Need to get existing dataset, or create new dataset (in dev)
        ds = get_create_dataset(cfg.bf, dsId)

        # Check that curation bot has manager access
        if not has_bf_access(ds):
            log.warning('UNABLE TO UPDATE DATASET DUE TO PERMISSIONS: {}'.format(dsId))
            continue

        # Need to clear dataset records/models 
        clear_dataset(cfg.bf, ds)
        recordCache = {m: {} for m in MODEL_NAMES}

        # Add data from the JSON file to the BF Dataset
        try:
            # Create all records
            add_data(cfg.bf, ds, dsId, recordCache, node)

            # Create all links between records
            add_links(cfg.bf, ds, dsId, recordCache, node)
        except BlackfynnException:
            log.error("Dataset {} failed to update".format(dsId))
            failedDatasets.append(dsId)
            continue
        # finally:
            # cfg.db_client.writeCache(dsId, recordCache)
            
        # Update Dataset Tags by copying TERMS Records
        tags =[]
        terms = get_bf_model(ds, 'term')
        term_records = terms.get_all()
        for term in term_records:
            tags.append(term.values['label'])

        ds.tags = list(set(tags+ds.tags))
        ds.update()

 


    # Timing stats
    duration = int((time() - new_start_time) * 1000)
    log.info("Added new metadata in {} milliseconds".format(duration))
    duration = int((time() - update_start_time) * 1000)
    log.info("Update datasets in {} milliseconds".format(duration))



    # Update dashboard when complete when running in production.
    if cfg.env == 'prod':
        update_sparc_dashboard(cfg.bf)

    return 

### CORE METHODS

def update_records(bf, ds, subNode, model_name, recordCache, model_create_fnc, transform_fnc):
    """Creates records for particular Model in Dataset

    This method takes the subNode for a particular model in a dataset and create the records.
    
    Parameters
    ----------
    bf: Blackfynn
        Blackfynn session
    ds: BF_Dataset
        Dataset that contains the records
    subNode: Dict
        JSON subnode for specific model in specific dataset
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
    for recordId, subNode in subNode.items():
        record_list.append(transform_fnc(recordId, subNode))
        json_id_list.append("{}".format( recordId ))

    model = model_create_fnc(bf, ds)
    if len(record_list):
        log.info('Creating {} new {} Records'.format(len(record_list), model_name))
        recordCache[model_name].update(zip(json_id_list, model.create_records(record_list)))

def add_data(bf, ds, dsId, recordCache, node):
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
        JSON subNode for dataset

    """

    # Get Models
    models = ds.models()

    # Adding all records without setting linked properties and relationships
    add_protocols(bf, ds, recordCache, node['Protocols'])
    add_terms(bf, ds, recordCache, node['Terms'])
    add_researchers(bf, ds, recordCache, node['Researcher'])
    add_subjects(bf, ds, recordCache, node['Subjects'])
    add_samples(bf, ds, recordCache, node['Samples'])
    add_awards(bf, ds, recordCache, node['Awards'])
    add_summary(bf, ds, recordCache, node['Resource'])

def add_links(bf, ds, dsId, recordCache, node):
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
        JSON subNode for dataset

    """
    
    # Adding all linked properties and relationships to records
    add_summary_links(bf,ds, recordCache, node['Resource'])
    add_subject_links(bf, ds, recordCache, node['Subjects'])
    add_sample_links(bf,ds, recordCache, node['Samples'])

def add_random_terms(ds, label, recordCache):
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
 
    record = add_random_terms.term_model.create_record({'label': label})
    recordCache['term'][label] = record
    return record

def add_record_links(ds, recordCache, model, record, links):
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

    log.info('Adding Record Linked Properties for {}'.format(record.id))
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
        targetType = get_bf_model(ds, linkedProp.target).type

        for item in valueList:
            # Check if value is in the record cache
            if item in recordCache[targetType]:
                # Record in cache --> exists in platform as a record
                linkedRec = recordCache[targetType][item]
            else:
                # Record not in cache --> check if term --> if so, add new term,
                # if not --> throw warning and don't link entry
                if targetType == 'term':
                    linkedRec = add_random_terms(ds, item, recordCache)
                else:
                    log.warning('Unable to link to non-existing record {}'.format(targetType))
                    continue
            
            # Try to link record to property
            try:
                record.add_linked_value(linkedRec.id, linkedProp)
            except Exception as e:
                log.error("Failed to add linked value '{}'='{}' to record {} with error '{}'".format(name, value, record, str(e)))
                raise BlackfynnException(e)

def add_record_relationships(ds, recordCache, record, relationships):
    
    log.info('Adding Record Relationships for {}'.format(record.id))
    # Iterate over all relationships in a record
    for name, value in relationships.items():
        targetRecordList = []

        targetModel = value['type']
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
        for item in valueList:

            # Lookup record in cache
            if item in recordCache[targetModel]:
                targetRecordList.append(recordCache[targetModel][item])
            elif targetModel == 'term':
                linkedRec = add_random_terms(ds, item, recordCache)
                targetRecordList.append(linkedRec)
            else:
                log.warning('Unable to relate to non-existing record {}'.format(targetModel))
                continue    

        # Add to list
        if len(targetRecordList) > 0:
            record.relate_to(targetRecordList, name)

### MODEL SPECIFIC METHODS

def add_protocols(bf, ds, recordCache, subNode):
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

    def transform(recordId, subNode):
        return {
             'label': subNode.get('label', '(no label)'),
             'url': recordId, #subNode.get('http://www.w3.org/2002/07/owl#sameAs'),
             'date': subNode.get('date'),
             'publisher': subNode.get('publisher'),
             'protocolHasNumberOfSteps': subNode.get('protocolHasNumberOfSteps'),
             'hasNumberOfProtcurAnnotations': subNode.get('hasNumberOfProtcurAnnotations')
        }

    update_records(bf, ds, subNode, "protocol", recordCache,  create_model, transform)

def add_terms(bf, ds, recordCache, subNode):
    log.info("Adding terms...")

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
        
    def transform(recordId, term):
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

    update_records(bf, ds, subNode, "term", recordCache,  create_model, transform)

def add_researchers(bf, ds, recordCache, subNode):
    log.info("Adding researchers...")

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

    def transform(recordId, subNode):
        return {
            'lastName': subNode.get('lastName', '(no label)'),
            'firstName': subNode.get('firstName'),
            'middleName': subNode.get('middleName'),
            'hasAffiliation': subNode.get('hasAffiliation'),
            'hasRole': subNode.get('hasRole'),
            'hasORCIDId': subNode.get('hasORCIDId')
        }

    update_records(bf,ds,subNode, "researcher", recordCache,  create_model, transform)

def add_subjects(bf, ds, recordCache, subNode):
    log.info("Adding subjects...")
    termModel = get_bf_model(ds, 'term')

    ## Define Model Generators
    def create_human_model(bf, ds):
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
                LinkedModelProperty('hasBiologicalSex', termModel, 'Biological sex'), # list (this is a bug)
                LinkedModelProperty('hasAgeCategory', termModel, 'Age category'),
                LinkedModelProperty('specimenHasIdentifier', termModel, 'Identifier'),
            ]
            )

    def create_animal_model(bf, ds):
        return get_create_model(bf, ds, 'animal_subject', 'Animal Subject',
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
            'subjectHasWeight': parse_unit_value(subNode, 'subjectHasWeight', 'kg'),
            'subjectHasHeight': parse_unit_value(subNode, 'subjectHasHeight'),
            'hasAge': parse_unit_value(subNode, 'hasAge', 's'),
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
            'hasAge': parse_unit_value(subNode, 'hasAge', 's'),
            'spatialLocationOfModulator': subNode.get('spatialLocationOfModulator'),
            'stimulatorUtilized': subNode.get('stimulatorUtilized'),
            'hasAssignedGroup': subNode.get('hasAssignedGroup'),
            'providerNote': subNode.get('providerNote'),
            'raw/involvesAnatomicalRegion': subNode.get('raw/involvesAnatomicalRegion'),
            'hasGenotype': subNode.get('hasGenotype'),
            'animalSubjectHasWeight': parse_unit_value(subNode, 'animalSubjectHasWeight'),
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
        log.info('Creating {} new human_subject Records'.format(len(human_record_list)))
        human_model = create_human_model(bf, ds)
        recordCache['human_subject'].update(zip(human_json_id_list,human_model.create_records(human_record_list)))

    elif len(animal_record_list) > 0:
        log.info('Creating {} new animal_subject Records'.format(len(animal_record_list)))
        animal_model = create_animal_model(bf, ds)
        recordCache['animal_subject'].update(zip(animal_json_id_list,animal_model.create_records(animal_record_list)))

def add_subject_links(bf, ds, recordCache, subNode): 

    model = None
    subtype = subNode.get('animalSubjectIsOfSpecies')
    try:
        if subtype == 'homo sapiens':
            model = get_bf_model(ds, 'human_subject')
        else:
            model = get_bf_model(ds, 'animal_subject')
    except:
        # No models for subject defined
        return


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

        add_record_links(ds, recordCache, model, record, links)

def add_samples(bf, ds, recordCache, subNode):
    log.info("Adding samples to dataset: {}".format(ds.id))

    def create_sample_model(bf, ds):
        
        
        
        return get_create_model(bf, ds, 'sample', 'Sample',
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
            ])

    def transform(recordId,subNode):
        return {
            'localId': subNode.get('localId', '(no label)'),
            'description': get_first(subNode, 'description'),
            'hasAssignedGroup': subNode.get('hasAssignedGroup'),
            'hasDigitalArtifactThatIsAboutIt': subNode.get('hasDigitalArtifactThatIsAboutIt'),
            'label': subNode.get('label'),
            'localExecutionNumber': subNode.get('localExecutionNumber'),
            'providerNote': subNode.get('providerNote')
        }

    update_records(bf,ds,subNode, "sample", recordCache,  create_sample_model, transform)

def add_sample_links(bf, ds, recordCache, subNode):

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
            subModel = get_create_model(bf, ds, 'subject', 'Subject',
                schema=[
                    ModelProperty('localId', 'ID', title=True)
                ]
                )
    
        return get_create_model(bf, ds, 'sample', 'Sample', linked=[
                LinkedModelProperty('extractedFromAnatomicalRegion', get_bf_model(ds, 'term'), 'Extracted from anatomical region'),
                LinkedModelProperty('wasDerivedFromSubject', subModel, 'Derived from subject')
            ])

    def transform_sample(subNode):
        subjectId = None
        if 'wasDerivedFromSubject' in subNode:
            regex = re.compile(r'.*/subjects/(.+)')
            subjectId = regex.match(subNode['wasDerivedFromSubject']).group(1)

        links = {
            'wasDerivedFromSubject': subjectId,
            'extractedFromAnatomicalRegion': subNode.get('raw/wasExtractedFromAnatomicalRegion'),
        }
        return links

    # Add Property links to model
    model = updateModel(bf, ds)

    # Iterate over multiple subject records, single dataset
    for sampleId, subjNode in subNode.items():
        record = get_record_by_id(sampleId, model, recordCache)
        links = transform_sample(subjNode)
        add_record_links(ds, recordCache, model, record, links)
    
        # Associate files with Samples
        if subNode.get('hasDigitalArtifactThatIsAboutIt') is not None:
            for fullFileName in subNode.get('hasDigitalArtifactThatIsAboutIt'):
                log.info('Adding File Links: {}'.format(fullFileName))
                filename, file_extension = os.path.splitext(fullFileName)
                pkgs = ds.get_packages_by_filename(filename)
                if len(pkgs) > 0:
                    for pkg in pkgs:
                        pkg.relate_to(record)

def add_summary(bf, ds, recordCache, subNode):
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

    def transform(recordId, subNode):
        # Check Milestone Completion Data is a date:
        try:
            milestoneDate = parse(subNode.get('milestoneCompletionDate'))
            try:
                milestoneDate = milestoneDate.isoformat()
            except:
                log.warning('Cannot parse the Milestone Date: {}'.format(subNode.get('milestoneCompletionDate')))
                milestoneDate = None
        except:
            milestoneDate = None

        return {
            'milestoneCompletionDate': milestoneDate,
            'isDescribedBy': get_as_list(subNode, 'isDescribedBy'),
            'acknowledgements': subNode.get('acknowledgements'),
            'collectionTitle': subNode.get('collectionTitle'),
            'curationIndex': subNode.get('curationIndex'),
            'description': get_as_list(subNode, 'description'),
            'errorIndex': subNode.get('errorIndex'),
            'hasExperimentalModality': get_as_list(subNode, 'hasExperimentalModality'),
            'hasNumberOfContributors': subNode.get('hasNumberOfContributors'),
            'hasNumberOfDirectories': subNode.get('hasNumberOfDirectories'),
            'hasNumberOfFiles': subNode.get('hasNumberOfFiles'),
            'hasNumberOfSamples': subNode.get('hasNumberOfSamples'),
            'hasNumberOfSubjects': subNode.get('hasNumberOfSubjects'),
            'hasSizeInBytes': subNode.get('hasSizeInBytes'),
            'label': subNode.get('label'),
            'submissionIndex': subNode.get('submissionIndex'),
            'title': subNode.get('title')
        }

    
    # links = {}

    # relations = {}
    # get "is about" relationships

    # links['hasAwardNumber'] = subNode['hasAwardNumber'] if ('hasAwardNumber' in subNode and subNode['hasAwardNumber'] in recordCache['award']) else None

    # regex = re.compile(r'\w+:\w+')
    # for value in subNode.get('http://purl.obolibrary.org/obo/IAO_0000136', []):
    #     if regex.match(value):
    #         relations.setdefault('is-about', []).append(value)

    # # get "involves anatomical region" relationships
    # for value in subNode.get('involvesAnatomicalRegion', []):
    #     relations.setdefault('involves-anatomical-region', []).append(value)

    # # get "protocol employs technique" relationships
    # for value in subNode.get('protocolEmploysTechnique', []):
    #     relations.setdefault('protocol-employs-technique', []).append(value)

    record_list = []
    json_id_list = []
    
    # No iteration because there is only one summary.
    record_list.append(transform('summary', subNode))
    json_id_list.append("{}".format( 'summary' ))

    if len(record_list):
        log.info('Creating {} new summary Records'.format(len(record_list)))
        model = create_model(bf, ds)
        recordCache['summary'].update(zip(json_id_list, model.create_records(record_list)))

def add_summary_links(bf, ds, recordCache, subNode):

    model = get_bf_model(ds, 'summary')

    def updateModel(bf, ds):
        return get_create_model(bf, ds, 'summary', 'Summary', linked=[
                LinkedModelProperty('hasAwardNumber', get_bf_model(ds, 'award'), 'Award number')
            ])

    def transform(subNode):
        links = {
            'hasAwardNumber': subNode.get('hasAwardNumber'),
        }
        relationships = {
            'hasResponsiblePrincipalInvestigator': {'type': 'researcher', 'node': subNode.get('hasResponsiblePrincipalInvestigator')},
            'hasContactPerson': {'type': 'researcher', 'node': subNode.get('hasContactPerson')},
            'involvesAnatomicalRegion': {'type': 'term', 'node': subNode.get('involvesAnatomicalRegion')},
            'protocolEmploysTechnique': {'type': 'term', 'node': subNode.get('protocolEmploysTechnique')},
            'isAbout': {'type': 'term', 'node': subNode.get('http://purl.obolibrary.org/obo/IAO_0000136')}

        }
        return {
            'links':links, 
            'relationships':relationships}
    
    # Add Property links to model
    model = updateModel(bf, ds)

    record = get_record_by_id('summary', model, recordCache)
    out = transform(subNode)
    add_record_links(ds, recordCache, model, record, out['links'] )

    # Create relationships
    rels = out['relationships']
    add_record_relationships(ds, recordCache, record, out['relationships'])

def add_awards(bf, ds, recordCache, subNode):
    log.info("Adding awards...")

    def create_model(bf, ds):
        return get_create_model(bf, ds, 'award', 'Award', schema=[
            ModelProperty('award_id', 'Award ID', title=True),
            ModelProperty('title', 'Title'),
            ModelProperty('description', 'Description'),
            ModelProperty('principal_investigator', 'Principal Investigator'),

        ])

    def transform(recordId, subNode):
        awardId = subNode.get('awardId')
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

    update_records(bf, ds, subNode, "award", recordCache,  create_model, transform)