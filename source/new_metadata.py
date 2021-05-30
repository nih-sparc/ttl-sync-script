#%%
import json
import logging
import re
import sys
import os

from rdflib import BNode, Graph, URIRef, term
from rdflib.namespace import RDF, RDFS, SKOS, OWL
from rdflib.compare import graph_diff

from base import (
    JSON_METADATA_FULL,
    JSON_METADATA_NEW,
    TTL_FILE_OLD,
    TTL_FILE_NEW,
    arrayProps,
    iri_lookup,
    strip_iri,
    get_recordset_hash
)

log = logging.getLogger(__name__)

# Create a new file-logger for this dataset
log_file_name = "/tmp/sparc-ttl-json.log"

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

#%% [markdown]
### Helper functions
#%%
def addEntry(output, datasetId):
    "Add a value for output[datasetId] if it doesn't already exist"
    output.setdefault(datasetId,
        {'summary':{},'contributor':{},'researcher':{},'subject':{},'protocol':{},'term':{},'sample':{}, 'award': {}, 'tag': []})

def parseMeasure(dsId, g, node, values):

    if (node, None, URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Measurement')) in g:
        # Current BNode is a measurement
        # preds = g.predicates(subject=node)
        # for v in preds:
        #     print('pred: {}'.format(v))
        #     values['unit'] = strip_iri(v)
        
        unit = strip_iri(g.value(subject=node, predicate=URIRef('http://uri.interlex.org/temp/uris/hasUnit')))
        values['unit'] = unit
            
        value = g.value(subject=node, predicate=URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#value'))
        values['value'] = str(value)

        if values['unit'] == 'dimensionless':
            log.warning("Measurement with no unit (value: {}) in {}".format(values['value'], dsId))

    elif (node, None, URIRef('http://www.w3.org/2000/01/rdf-schema#Datatype')) in g:
            # Current BNode is a rdfs:Datatype

            unit = strip_iri(g.value(subject=node, predicate=URIRef('http://www.w3.org/2002/07/owl#onDatatype')))
            values['unit'] = strip_iri(unit)

            value = g.value(subject=node, predicate=URIRef('http://www.w3.org/2002/07/owl#withRestrictions'))
            
            # Get Lower Bound Range
            first = g.value(subject=value, predicate=URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#first'))
            min_incl = g.value(subject=first, predicate=URIRef('http://www.w3.org/2001/XMLSchema#minInclusive'))
            
            #Get Higher Bound Range
            rest = g.value(subject=value, predicate=URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#rest'))
            rest_first = g.value(subject=rest, predicate=URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#first'))
            max_incl = g.value(subject=rest_first, predicate=URIRef('http://www.w3.org/2001/XMLSchema#maxInclusive'))

            values['value'] = "{}-{}".format(str(min_incl), str(max_incl))

            if values['unit'] == 'dimensionless':
                log.warning("Measurement with no unit (value: {}) in {}".format(values['value'], dsId))

    else:
        log.warning("Encountered a B-Node that is not a measurement in {}".format(dsId))

    return values


#%% [markdown]
### PopulateValue

# g: graph
# datasetId: datasetId
# ds: output for particular dataset
# data: output for particular section in dataset
# p: predicate
# o: object
# iriCache: cache for iri terms.

#%%
def populateValue(g, datasetId, ds, data, p, o, iriCache):

    ## Skipping following IRI's as they are handled separately (getResearcher, getProtocols, etc.)
    skipIri = [
        term.URIRef('http://uri.interlex.org/temp/uris/contributorTo'),
        term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
        term.URIRef('http://uri.interlex.org/temp/uris/hasUriApi'),
        term.URIRef('http://uri.interlex.org/temp/uris/hasUriHuman'),
        term.URIRef('http://uri.interlex.org/temp/uris/hasProtocol'),
        term.URIRef('http://uri.interlex.org/temp/uris/wasUpdatedAtTime')]
    key = strip_iri(p.strip())

    if p in skipIri:
        return

    if isinstance(o, term.URIRef):
        value = iri_lookup(g, o.strip(), iriCache)
        if value:
            if isinstance(value, dict) and 'curie' in value:
                ds['term'][value['curie']] = value
                value = value['curie']
            # if isinstance(value, dict) and 'iri' in value:
            #     key = strip_iri(value['iri'])
            #     ds['term'][key] = value
            #     value = key

            if key in arrayProps:
                array = data.setdefault(key, [])
                array.append(value)
            
            else:
                if key in data:
                    log.warning('Unexpected creation of array for:  %s - %s - %s', datasetId, key, value)
                    log.warning('Existing value for this key     :  %s - %s - %s', datasetId, key, data[key])
                    log.warning('----- Will use the first value after sorting the array -----')
                    sorted_values = [value, data[key]]
                    sorted_values.sort()
                    data[key] = sorted_values[0]

                else:
                    data[key] = value

    elif isinstance(o, term.Literal):
        value = strip_iri(o.strip())
        if key in arrayProps:
            array = data.setdefault(key, [])
            array.append(value)
        else:
            if key in data:
                log.warning('Unexpected creation of array for:  %s - %s - %s', datasetId, key, value)
                log.warning('Existing value for this key     :  %s - %s - %s', datasetId, key, data[key])
                log.warning('----- Will use the first value after sorting the array -----')
                sorted_values = [value, data[key]]
                sorted_values.sort()
                data[key] = sorted_values[0]

            else:
                data[key] = value

    elif isinstance(o, term.BNode):
        data[key] = parseMeasure(datasetId, g, o, {'value': '', 'unit': ''})

    else:
        raise Exception('Unknown RDF term: %s' % type(o))

def getDatasets(gNew, gDelta, output, iriCache):
    # Iterate over Datasets
    for ds in gNew.subjects(RDF.type, URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Dataset')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", ds)
        datasetId = strip_iri(m.group(0).strip())
        addEntry(output, datasetId)
        log.info("Adding dataset: " + datasetId)
        for p, o in gDelta.predicate_objects(ds):
            if p == URIRef("http://uri.interlex.org/temp/uris/hasAwardNumber"):
                getAwards(o, datasetId, output)
            populateValue(gDelta, datasetId, output[datasetId], output[datasetId]['summary'], p, o, iriCache)

# def getContributors(gNew, gDelta, output, iriCache):
#     # Iterate over Researchers
#     for s, o in gNew.subject_objects(URIRef('http://uri.interlex.org/temp/uris/aboutContributor')):
#         log.info('s:{}'.format(s))
#         log.info('o:{}'.format(o))
#         # m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", o)
#         # datasetId = stripIri(m.group(0).strip())
#         # user = s.split('/')[-1] # either a blackfynn user id or "Firstname-Lastname"
#         # newEntry = {}
#         # log.info(gDelta.predicate_objects(s))
#         for p2, o2 in gDelta.predicate_objects(s):
#             populateValue(gDelta, datasetId, output[datasetId], newEntry, p2, o2, iriCache)
#         if newEntry:
#             output[datasetId]['Contributor'][user] = newEntry

def getResearchers(gNew, gDelta, output, iriCache):
    # Iterate over Researchers
    for s, o in gNew.subject_objects(URIRef('http://uri.interlex.org/temp/uris/contributorTo')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", o)
        datasetId = strip_iri(m.group(0).strip())
        user = strip_iri(s)
        # user = s #s.split('/')[-1] # either a blackfynn user id or "Firstname-Lastname"
        newEntry = {}
        for p2, o2 in gDelta.predicate_objects(s):
            populateValue(gDelta, datasetId, output[datasetId], newEntry, p2, o2, iriCache)
        if newEntry:
            output[datasetId]['researcher'][user] = newEntry

def getSubjects(gNew, gDelta, output, iriCache):
    # Iterate over Subjects
    for s in gNew.subjects(RDF.type, URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Subject')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)/subjects/(?P<sub>[\w%-]+)", s)
        datasetId = m.group(1).strip()
        subj_id = m.group(2).strip()
        output[datasetId]['subject'][subj_id] = {}
        for p2, o2 in gDelta.predicate_objects(s):
            populateValue(gDelta, datasetId, output[datasetId],output[datasetId]['subject'][subj_id], p2, o2, iriCache)

def getSamples(gNew, gDelta, output, iriCache):
    # Iterate over Samples
    for s in gNew.subjects(RDF.type, URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Sample')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)/samples/(?P<sub>.*$)", s)
        datasetId = m.group(1).strip()
        sampleId = m.group(2).strip()
        newEntry = {}
        for p2, o2 in gDelta.predicate_objects(s):
            populateValue(gDelta, datasetId, output[datasetId], newEntry, p2, o2, iriCache)
        if newEntry:
            output[datasetId]['sample'][sampleId] = newEntry

def getProtocols(gNew, gDelta, output, iriCache):
    # Iterate over Protocols
    for s, o in gNew.subject_objects(URIRef('http://uri.interlex.org/temp/uris/hasProtocol')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", s)
        datasetId = strip_iri(m.group(0).strip())
        url = str(o)
        newEntry = {}
        for p2, o2 in gDelta.predicate_objects(o):
            populateValue(gDelta, datasetId, output[datasetId], newEntry, p2, o2, iriCache)
        if newEntry:
            output[datasetId]['protocol'][url] = newEntry

def getAwards(awardIdURI, dsId, output):
    # Iterate over awards
    awardId = strip_iri(awardIdURI)
    output[dsId]['award'][awardId] = {
        'awardId': awardId
    }

def getTags(gNew, gDelta, output, iriCache):
    # Iterate over Protocols
    for s, o in gNew.subject_objects(URIRef('http://purl.obolibrary.org/obo/IAO_0000136')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", s)
        if m:
            if isinstance(o, term.URIRef):
                t = iri_lookup(gNew, o, iriCache)
                if t:
                    if isinstance(t, str):
                        tag = t
                    else:
                        tag = t['labels'][0]
                else:
                    continue
            else:
                tag = str(o)

            datasetId = strip_iri(m.group(0).strip())
            if tag not in output[datasetId]['tag']:
                output[datasetId]['tag'].append(tag)

def sort_output(input):
    """ Recursively sort all arrays in input
    """

    for key, value in input.items():
        if isinstance(value, list):
            value.sort()
        elif isinstance(value, dict):
            sort_output(value)

def compute_hash_for_records(input):
    """ Add a hash value for each record"""

    for ds_is, dataset in input.items():
        for model in {'award', 'contributor', 'protocol', 'researcher','sample','subject', 'term'}:
            if model in dataset:
                for key, value in dataset[model].items():
                    dataset[model][key]['hash'] = get_recordset_hash(value)

def buildJson(version):
    log.info('Building new meta data json')

    output_file = JSON_METADATA_FULL
    input_file = TTL_FILE_NEW
    if version < 0:
        output_file = "{}_{}.json".format(output_file[:-5], version)
        input_file = "{}_{}.ttl".format(input_file[:-4], version)

    gNew = Graph().parse(input_file, format='turtle')

    output = {}
    iriCache = {}

    log.info('Getting datasets...')
    getDatasets(gNew, gNew, output, iriCache)

    log.info("The properties below are expected to be of type array.")
    log.info(arrayProps)
    log.info(output)

    # log.info('Getting Contributors...')
    # getContributors(gNew, gDelta, output, iriCache)

    log.info('Getting tags...')
    getTags(gNew, gNew, output, iriCache)

    log.info('Getting Researchers...')
    getResearchers(gNew, gNew, output, iriCache)

    log.info('Getting Subjects...')
    getSubjects(gNew, gNew, output, iriCache)

    log.info('Getting Samples...')
    getSamples(gNew, gNew, output, iriCache)

    log.info('Getting Protocols...')
    getProtocols(gNew, gNew, output, iriCache)
    del iriCache

    # Sort all arrays
    log.info("Sorting all arrays in output")
    sort_output(output)

    # Compute hash for all records
    log.info("Compute hash for all records")
    compute_hash_for_records(output)

    with open(output_file, 'w') as f:
        json.dump(output, f, sort_keys=True)
        log.info("Added %d datasets to '%s'", len(output), f.name)
