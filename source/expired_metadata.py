'''
> expired_metadata.py

Compares two SPARC TTLs and get the expired metadata
(triples contained in the old TTL but not the new one)

Then exports a JSON object with the following structure:
{
    datasetId: {
        "expired": true/false,
        "records": {
            model name: {
                identifier: {
                    "expired": true/false,
                    "values": [values],
                    "arrayValues": {property: [values]}
                }
            }
        }
    }
}
'''
#%%
import json
import logging
import re

from rdflib import Graph, BNode, Literal, URIRef
from rdflib.namespace import RDF, RDFS, SKOS, OWL
import requests

from base import (
    JSON_METADATA_EXPIRED,
    TTL_FILE_NEW,
    TTL_FILE_OLD,
    arrayProps,
    iri_lookup,
    strip_iri
)

log = logging.getLogger(__name__)


#%% [markdown]
### Random helpers

#%%
def addDataset(output, datasetId):
    return output.setdefault(datasetId, {'expired': False, 'records': {}})

def addRecord(output, datasetId, model, identifier):
    d = addDataset(output, datasetId)['records']
    return d.setdefault(model, {}) \
            .setdefault(identifier, {'expired': False, 'values': [], 'arrayValues': {}})

def addValue(output, datasetId, model, identifier, prop, overwrite=False):
    r = addRecord(output, datasetId, model, identifier)
    if prop in r['values']:
        msg = "Duplicate property '%s' for record %s (model= %s, datasetId= %s" % \
            (prop, identifier, model, datasetId)
        if overwrite:
            log.warning(msg)
            return
        log.error(msg)
        raise Exception("Property '%s' is already present!" % prop)
    r['values'].append(prop)

def addArrayValue(output, datasetId, model, identifier, prop, value, overwrite=False):
    r = addRecord(output, datasetId, model, identifier)
    p = r['arrayValues'].setdefault(prop, [])
    if value in p:
        msg = "Duplicate value '%s=%s' for record %s (model= %s, datasetId= %s" % \
            (prop, value, identifier, model, datasetId)
        if overwrite:
            log.warning(msg)
            return
        log.error(msg)
        raise Exception("Value '%s' is already present!" % prop)
    p.append(value)

def populateExpiredValue(output, datasetId, model, identifier, p, o, iriCache):
    skipIri = (
        URIRef('http://uri.interlex.org/temp/uris/contributorTo'),
        URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
        URIRef('http://uri.interlex.org/temp/uris/hasUriApi'),
        URIRef('http://uri.interlex.org/temp/uris/hasUriHuman'),
        URIRef('http://uri.interlex.org/temp/uris/hasProtocol'))
    if p in skipIri:
        return
    key = strip_iri(p.strip())

    if isinstance(o, URIRef):
        value = iri_lookup(o.strip(), iriCache)
        if not value:
            return

        record = addRecord(output, datasetId, model, identifier)
        if key in arrayProps:
            if isinstance(value, dict) and 'curie' in value:
                term = addRecord(output, datasetId, 'term', value['curie'])
                term['expired'] = True
                v = value['curie']
            else:
                v = value
            addArrayValue(output, datasetId, model, identifier, key, v)

        elif key not in record['values']:
            if isinstance(value, dict) and 'curie' in value:
                term = addRecord(output, datasetId, 'term', value['curie'])
                term['expired'] = True
            addValue(output, datasetId, model, identifier, key)
        else:
            log.warning('I overwrote an existing entry!  %s - %s - %s', datasetId, key, value)
            return
            #raise Exception('I just almost overwrote an existing entry!')

    elif isinstance(o, Literal):
        record = addRecord(output, datasetId, model, identifier)
        if key in arrayProps:
            value = strip_iri(o.strip())
            addArrayValue(output, datasetId, model, identifier, key, value)
        elif key not in record['values']:
            addValue(output, datasetId, model, identifier, key)
        else:
            log.warning('I overwrote an existing entry! %s - %s', datasetId, key)
            #raise Exception('I just almost overwrote an existing entry!')

    elif isinstance(o, BNode):
        record = addRecord(output, datasetId, model, identifier)
        addValue(output, datasetId, model, identifier, key, overwrite=True)

    else:
        raise Exception('Unknown value type: %s' % type(o))


#%% [markdown]
### Get expired datasets and records
##### (except terms)

#%%
def getExpiredDatasets(g, output):
    for ds in g.subjects(RDF.type, URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Resource')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", ds)
        datasetId = strip_iri(m.group(0).strip())
        d = addDataset(output, datasetId)
        d['expired'] = True

def getExpiredResearchers(g, output):
    for s, o in g.subject_objects(URIRef('http://uri.interlex.org/temp/uris/contributorTo')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", o)
        datasetId = strip_iri(m.group(0).strip())
        user = s.split('/')[-1] # either a blackfynn user id or "Firstname-Lastname"
        r = addRecord(output, datasetId, 'researcher', user)
        r['expired'] = True

def getExpiredSubjects(g, output):
    for s in g.subjects(RDF.type, URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Subject')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)/subjects/(?P<sub>[\w-]+)", s)
        datasetId = m.group(1).strip()
        subjId = m.group(2).strip()
        r = addRecord(output, datasetId, 'subject', subjId)
        r['expired'] = True

def getExpiredSamples(g, output):
    for s in g.subjects(RDF.type, URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Sample')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)/samples/(?P<sub>.*$)", s)
        datasetId = m.group(1).strip()
        sampleId = m.group(2).strip()
        r = addRecord(output, datasetId, 'sample', sampleId)
        r['expired'] = True

def getExpiredProtocols(g, output):
    for s, o in g.subject_objects(URIRef('http://uri.interlex.org/temp/uris/hasProtocol')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", s)
        datasetId = strip_iri(m.group(0).strip())
        protocolUrl = str(o)
        r = addRecord(output, datasetId, 'protocol', protocolUrl)
        r['expired'] = True

def getExpiredAwards(g, output):
    pass
    # TODO: for s,p,o in g.subject_objects(URIRef('http://uri.interlex.org/temp/uris/awards')):


#%% [markdown]
### Get expired Terms and record properties

#%%
def getExpiredDatasetProperties(gIntersect, gDelta, output, iriCache):
    for ds in gIntersect.subjects(RDF.type, URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Resource')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", ds)
        datasetId = strip_iri(m.group(0).strip())
        for p, o in gDelta.predicate_objects(ds):
            populateExpiredValue(output, datasetId, 'summary', datasetId, p, o, iriCache)

def getExpiredResearcherProperties(gIntersect, gDelta, output, iriCache):
    for s,p,o in gIntersect.triples( (None, URIRef('http://uri.interlex.org/temp/uris/contributorTo'), None) ):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", o)
        datasetId = strip_iri(m.group(0).strip())
        user = s.split('/')[-1] # either a blackfynn user id or "Firstname-Lastname"
        for p2, o2 in gDelta.predicate_objects(s):
            populateExpiredValue(output, datasetId, 'researcher', user, p2, o2, iriCache)

def getExpiredSubjectProperties(gIntersect, gDelta, output, iriCache):
    for s in gIntersect.subjects(RDF.type, URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Subject')):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)/subjects/(?P<sub>[\w-]+)", s)
        datasetId = m.group(1).strip()
        subjId = m.group(2).strip()
        for p2, o2 in gDelta.predicate_objects(s):
            populateExpiredValue(output, datasetId, 'subject', subjId, p2, o2, iriCache)

def getExpiredSampleProperties(gIntersect, gDelta, output, iriCache):
    for s in gIntersect.subjects(RDF.type, URIRef('http://uri.interlex.org/tgbugs/uris/readable/sparc/Sample')):

        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)/samples/(?P<sub>.*$)", s)
        datasetId = m.group(1).strip()
        sampleId = m.group(2).strip()
        for p2, o2 in gDelta.predicate_objects(s):
            populateExpiredValue(output, datasetId, 'sample', sampleId, p2, o2, iriCache)

def getExpiredProtocolProperties(gIntersect, gDelta, output, iriCache):
    for s,p,o in gIntersect.triples((None, URIRef('http://uri.interlex.org/temp/uris/hasProtocol'), None)):
        m = re.search(r".*(?P<ds>N:dataset:[:\w-]+)", s)
        datasetId = strip_iri(m.group(0).strip())
        protocolUrl = str(o)
        for p2, o2 in gDelta.predicate_objects(o):
            populateExpiredValue(output, datasetId, 'protocol', protocolUrl, p2, o2, iriCache)

def getExpiredAwardProperties(gIntersect, gDelta, output, iriCache):
    pass # TODO


#%% [markdown]
### Main body

#%%
def buildJson():
    # Create graphs:
    gOld = Graph().parse(TTL_FILE_OLD, format='turtle')
    gNew = Graph().parse(TTL_FILE_NEW, format='turtle')
    gDelta = gOld - gNew # contains expired triples
    gIntersect = gNew & gOld # contains triples shared between both graphs

    # Set namespace prefixes:
    for ns in (gOld + gNew).namespaces():
        gDelta.namespace_manager.bind(ns[0], ns[1])
        gIntersect.namespace_manager.bind(ns[0], ns[1])

    # Get expired datasets and metadata:
    iriCache = {}
    expired = {}

    log.info('Getting expired datasets...')
    getExpiredDatasets(gDelta, expired)

    log.info('Getting expired records...')
    getExpiredResearchers(gDelta, expired)
    getExpiredSubjects(gDelta, expired)
    getExpiredSamples(gDelta, expired)
    getExpiredProtocols(gDelta, expired)
    #getExpiredAwards(gDelta, expired)

    log.info('Getting expired record properties...')
    getExpiredDatasetProperties(gIntersect, gDelta, expired, iriCache)
    getExpiredResearcherProperties(gIntersect, gDelta, expired, iriCache)
    getExpiredSubjectProperties(gIntersect, gDelta, expired, iriCache)
    getExpiredSampleProperties(gIntersect, gDelta, expired, iriCache)
    getExpiredProtocolProperties(gIntersect, gDelta, expired, iriCache)
    #getExpiredAwardProperties(gIntersect, gDelta, expired, iriCache)
    del iriCache

    with open(JSON_METADATA_EXPIRED, 'w') as f:
        json.dump(expired, f)
        log.info("Added %d datasets to '%s'", len(expired), f.name)

if __name__ == '__main__':
    buildJson()
