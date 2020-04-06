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

from base import (
    JSON_METADATA_FULL,
    JSON_METADATA_NEW,
    TTL_FILE_OLD,
    TTL_FILE_NEW,
    arrayProps,
    iriLookup,
    stripIri,
)

log = logging.getLogger(__name__)


#%% [markdown]
### Helper functions
#%%
def addEntry(output, datasetId):
    "Add a value for output[datasetId] if it doesn't already exist"
    output.setdefault(datasetId,
        {'Resource':{},'Researcher':{},'Subjects':{},'Protocols':{},'Terms':{},'Samples':{}, 'Awards': {}})

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
            values['unit'] = stripIri(v)
        elif isinstance(v, term.BNode):
            parseMeasure(g, v, values)
        else:
            log.error('Bad measurement value gotten: %s', v)

    return values

def populateValue(g, datasetId, ds, data, p, o, iriCache):
    skipIri = [
        term.URIRef('http://uri.interlex.org/temp/uris/contributorTo'),
        term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
        term.URIRef('http://uri.interlex.org/temp/uris/hasUriApi'),
        term.URIRef('http://uri.interlex.org/temp/uris/hasUriHuman'),
        term.URIRef('http://uri.interlex.org/temp/uris/hasProtocol')]
    key = stripIri(p.strip())

    if isinstance(o, term.URIRef):
        if p in skipIri:
            return
        value = iriLookup(o.strip(), iriCache)
        if value:
            if isinstance(value, dict) and 'curie' in value:
                ds['Terms'][value['curie']] = value
                value = value['curie']

            if key in arrayProps:
                array = data.setdefault(key, [])
                array.append(value)
            else:
                if key in data:
                    log.warning('I overwrote an existing entry!  %s - %s - %s', datasetId, key, value)
                    #raise Exception('I just almost overwrote an existing entry!')
                data[key] = value

    elif isinstance(o, term.Literal):
        value = stripIri(o.strip())
        if key in arrayProps:
            array = data.setdefault(key, [])
            array.append(value)
        else:
            if key in data:
                log.warning('I overwrote an existing entry!  %s - %s - %s', datasetId, key, value)
                #raise Exception('I just almost overwrote an existing entry!')
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
        datasetId = stripIri(m.group(0).strip())
        addEntry(output, datasetId)
        for p, o in gDelta.predicate_objects(ds):
            if p == URIRef("http://uri.interlex.org/temp/uris/hasAwardNumber"):
                getAwards(o, datasetId, output)
            populateValue(gDelta, datasetId, output[datasetId], output[datasetId]['Resource'], p, o, iriCache)

def getResearchers(gNew, gDelta, output, iriCache):
    # Iterate over Researchers
    for s, o in gNew.subject_objects(URIRef('http://uri.interlex.org/temp/uris/contributorTo')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", o)
        datasetId = stripIri(m.group(0).strip())
        user = s.split('/')[-1] # either a blackfynn user id or "Firstname-Lastname"
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
        datasetId = stripIri(m.group(0).strip())
        url = str(o)
        newEntry = {}
        for p2, o2 in gDelta.predicate_objects(o):
            populateValue(gDelta, datasetId, output[datasetId], newEntry, p2, o2, iriCache)
        if newEntry:
            output[datasetId]['Protocols'][url] = newEntry

def getAwards(awardIdURI, dsId, output):
    # Iterate over awards
    awardId = stripIri(awardIdURI)
    output[dsId]['Awards'][awardId] = dsId

# TODO: make an 'Organization' model using ror.org API


#%% [markdown]
### Main body
#%%
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

    # Set namespace prefixes:
    log.info('Setting namespace prefixes')
    for ns in (gOld + gNew).namespaces():
        gDelta.namespace_manager.bind(ns[0], ns[1])
        gIntersect.namespace_manager.bind(ns[0], ns[1])

    output = {}
    iriCache = {}

    log.info('Getting datasets...')
    getDatasets(gNew, gDelta, output, iriCache)

    log.info('Getting new records and properties...')
    getResearchers(gNew, gDelta, output, iriCache)
    getSubjects(gNew, gDelta, output, iriCache)
    getSamples(gNew, gDelta, output, iriCache)
    getProtocols(gNew, gDelta, output, iriCache)
    del iriCache

    with open(outputFile, 'w') as f:
        json.dump(output, f)
        log.info("Added %d datasets to '%s'", len(output), f.name)

if __name__ == '__main__':
    if len(sys.argv != 2):
        raise Exception("Must use option 'diff' or 'full'")
    buildJson(sys.argv[1])