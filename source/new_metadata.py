'''
> new_metadata.py (full|diff)

With the 'diff' option, compares two SPARC TTLs and get new metadata
(triples contained in the new TTL but not the old one).

With the 'full' option, gets all metadata from a TTL_FILE_NEW.

Then exports a JSON object with the following structure:
{
    datasetId: {
        "Resource": { ... }
        "Researcher": { ... }
        "Subjects": { ... }
        "Protocols": { ... }
        "Terms": { ... }
        "Samples": { ... }
        "Tags": { ... }
    }
}
'''
#%%
import json
import logging
import re
import sys

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
    strip_iri
)

log = logging.getLogger(__name__)

#%% [markdown]
### Helper functions
#%%
def addEntry(output, datasetId):
    "Add a value for output[datasetId] if it doesn't already exist"
    output.setdefault(datasetId,
        {'Resource':{},'Contributor':{},'Researcher':{},'Subjects':{},'Protocols':{},'Terms':{},'Samples':{}, 'Awards': {}, 'Tags': []})

def parseMeasure(g, node, values):
    for v in g.objects(subject=node):
        if isinstance(v, term.Literal):
            if v.datatype in (
                             term.URIRef('http://www.w3.org/2001/XMLSchema#integer'),
                             term.URIRef('http://www.w3.org/2001/XMLSchema#double')):
                values['value'].append(str(v))
            else:
                log.warning("Literal '%s' has unrecognized datatype: '%s'", v, v.datatype)
        elif isinstance(v, term.URIRef):
            if v in (
                    term.URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Measurement'),
                    term.URIRef('http://www.w3.org/2000/01/rdf-schema#Datatype'),
                    term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#nil')):
                continue
            values['unit'] = strip_iri(v)
        elif isinstance(v, term.BNode):
            parseMeasure(g, v, values)
        else:
            log.error('Bad measurement value gotten: %s', v)

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
        term.URIRef('http://uri.interlex.org/temp/uris/hasProtocol')]
    key = strip_iri(p.strip())

    if isinstance(o, term.URIRef):
        if p in skipIri:
            return
        value = iri_lookup(o.strip(), iriCache)
        if value:
            if isinstance(value, dict) and 'curie' in value:
                ds['Terms'][value['curie']] = value
                value = value['curie']

            if key in arrayProps:
                array = data.setdefault(key, [])
                array.append(value)
            
            else:
                if key in data:
                    log.warning('Unexpected creation of array for:  %s - %s - %s', datasetId, key, value)
                    log.warning('Existing value for this key     :  %s - %s - %s', datasetId, key, data[key])
                    log.warning('----- continue to use initial value -----')
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
                log.warning('----- continue to use initial value -----')
            else:
                data[key] = value

    elif isinstance(o, term.BNode):
        data[key] = parseMeasure(g, o, {'value': [], 'unit': ''})

    else:
        raise Exception('Unknown RDF term: %s' % type(o))


#%% [markdown]
### Get each type of metadata:
#%%
def getDatasets(gNew, gDelta, output, iriCache):
    # Iterate over Datasets
    for ds in gNew.subjects(RDF.type, URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Resource')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", ds)
        datasetId = strip_iri(m.group(0).strip())
        addEntry(output, datasetId)
        for p, o in gDelta.predicate_objects(ds):
            if p == URIRef("http://uri.interlex.org/temp/uris/hasAwardNumber"):
                getAwards(o, datasetId, output)
            populateValue(gDelta, datasetId, output[datasetId], output[datasetId]['Resource'], p, o, iriCache)

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
        user = s #s.split('/')[-1] # either a blackfynn user id or "Firstname-Lastname"
        newEntry = {}
        for p2, o2 in gDelta.predicate_objects(s):
            populateValue(gDelta, datasetId, output[datasetId], newEntry, p2, o2, iriCache)
        if newEntry:
            output[datasetId]['Researcher'][user] = newEntry

def getSubjects(gNew, gDelta, output, iriCache):
    # Iterate over Subjects
    for s in gNew.subjects(RDF.type, URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Subject')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)/subjects/(?P<sub>[\w-]+)", s)
        datasetId = m.group(1).strip()
        subj_id = m.group(2).strip()
        output[datasetId]['Subjects'][subj_id] = {}
        for p2, o2 in gDelta.predicate_objects(s):
            populateValue(gDelta, datasetId, output[datasetId],output[datasetId]['Subjects'][subj_id], p2, o2, iriCache)

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
            output[datasetId]['Samples'][sampleId] = newEntry

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
            output[datasetId]['Protocols'][url] = newEntry

def getAwards(awardIdURI, dsId, output):
    # Iterate over awards
    awardId = strip_iri(awardIdURI)
    output[dsId]['Awards'][awardId] = {
        'awardId': awardId
    }

def getTags(gNew, gDelta, output, iriCache):
    # Iterate over Protocols
    for s, o in gNew.subject_objects(URIRef('http://purl.obolibrary.org/obo/IAO_0000136')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", s)
        if m:
            if isinstance(o, term.URIRef):
                t = iri_lookup('dsakjd', iriCache)
                if t:
                    tag = t['labels'][0]
                else:
                    continue
            else:
                tag = str(o)

            datasetId = strip_iri(m.group(0).strip())
            if tag not in output[datasetId]['Tags']:
                output[datasetId]['Tags'].append(tag)


def buildJson(_type):
    log.info('Building new meta data json')
    if _type == 'diff':
        outputFile = JSON_METADATA_NEW
        gOld = Graph().parse(TTL_FILE_OLD, format='turtle')
        gNew = Graph().parse(TTL_FILE_NEW, format='turtle')
    elif _type == 'full':
        outputFile = JSON_METADATA_FULL
        gOld = Graph()
        gNew = Graph().parse(TTL_FILE_NEW, format='turtle')
    else:
        raise Exception("Must use option 'diff' or 'full'")
    
    gDelta = gNew - gOld # contains expired triples
    gIntersect = gNew & gOld # contains triples shared between both graphs

    # gIntersect, gDeprecated, gDelta = graph_diff(gDelta1, gNew)

    # gDelta.serialize(destination='diff_graph_delta.ttl', format='turtle')
    # gDeprecated.serialize(destination='diff_graph_deprecated.ttl', format='turtle')

    # gDelta.serialize(destination='diff_graph.ttl', format='turtle')

    

    # Set namespace prefixes:
    log.info('Setting namespace prefixes')
    for ns in (gOld + gNew).namespaces():
        gDelta.namespace_manager.bind(ns[0], ns[1])
        gIntersect.namespace_manager.bind(ns[0], ns[1])

    output = {}
    iriCache = {}

    log.info('Getting datasets...')
    getDatasets(gNew, gDelta, output, iriCache)

    # log.info('Getting Contributors...')
    # getContributors(gNew, gDelta, output, iriCache)

    log.info('Getting tags...')
    getTags(gNew, gDelta, output, iriCache)

    log.info('Getting Researchers...')
    getResearchers(gNew, gDelta, output, iriCache)
    
    log.info('Getting Subjects...')
    getSubjects(gNew, gDelta, output, iriCache)
    
    log.info('Getting Samples...')
    getSamples(gNew, gDelta, output, iriCache)
    
    log.info('Getting Protocols...')
    getProtocols(gNew, gDelta, output, iriCache)
    del iriCache

    with open(outputFile, 'w') as f:
        json.dump(output, f)
        log.info("Added %d datasets to '%s'", len(output), f.name)
