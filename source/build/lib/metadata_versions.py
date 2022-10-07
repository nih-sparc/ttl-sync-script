'Download the two latest SPARC TTLs'
from shutil import copyfileobj
import logging

from bs4 import BeautifulSoup
import requests
from requests.compat import quote, unquote
from rdflib import BNode, Graph, URIRef, term
from rdflib.namespace import RDF, RDFS, SKOS, OWL

from base import (
    TTL_FILE_NEW,
    TTL_FILE_OLD,
    TTL_FILE_DIFF
)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

BASE_URL = 'https://cassava.ucsd.edu/sparc/exports/'

def getVersion(offset_from_latest):
    r = requests.get(BASE_URL)
    soup = BeautifulSoup(r.text, 'html.parser')
    hrefs = list((x.get('href') for x in soup.find_all(href=lambda x: x and not x.startswith('.'))))
    return unquote(hrefs[offset_from_latest].strip('/'))

def latest_version():
    r = requests.get(BASE_URL)
    soup = BeautifulSoup(r.text, 'html.parser')
    hrefs = (x.get('href') for x in soup.find_all(href=lambda x: x and not x.startswith('.')))  
    return unquote(max(hrefs)).strip('/')

def getTTL(version, filename):
    '''Get a version of the sparc metadata file, save it to `filename`'''
    url = BASE_URL + quote(version) + '/curation-export.ttl'
    log.info(url)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            copyfileobj(r.raw, f)

def getSpecificTTLVersion(version):
    file_name = "{}_{}.ttl".format(TTL_FILE_NEW[:-4], version)
    getTTL(getVersion(version), file_name)
    return file_name

def getLatestTTLVersion():
    getTTL((latest_version()), TTL_FILE_NEW)
    return TTL_FILE_NEW

    
