'Download the two latest SPARC TTLs'
from shutil import copyfileobj

# import boto3
from bs4 import BeautifulSoup
#from moto import mock_ssm
import requests
from requests.compat import quote, unquote
from rdflib import BNode, Graph, URIRef, term
from rdflib.namespace import RDF, RDFS, SKOS, OWL

# from base import SSMClient
# from config import Configs

from base import (
    TTL_FILE_NEW,
    TTL_FILE_OLD,
    TTL_FILE_DIFF
)

BASE_URL = 'https://cassava.ucsd.edu/sparc/archive/exports/'
# cfg = Configs()
# ssm = SSMClient()

def getVersion(offset_from_latest):
    r = requests.get(BASE_URL)
    soup = BeautifulSoup(r.text, 'html.parser')
    hrefs = list((x.get('href') for x in soup.find_all(href=lambda x: x and not x.startswith('.'))))
    return unquote(hrefs[offset_from_latest].strip('/'))

def latestVersion():
    r = requests.get(BASE_URL)
    soup = BeautifulSoup(r.text, 'html.parser')
    hrefs = (x.get('href') for x in soup.find_all(href=lambda x: x and not x.startswith('.')))
    return unquote(max(hrefs)).strip('/')

def setLastUpdated(cfg, newVersion):
    cfg.ssm.set('last_updated', newVersion)

def getLastUpdated(cfg):
    return cfg.ssm.get('last_updated')

def getTTL(version, filename):
    '''Get a version of the sparc metadata file, save it to `filename`'''
    url = BASE_URL + quote(version) + '/curation-export.ttl'
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            copyfileobj(r.raw, f)

def create_diff_ttl():
    gOld = Graph().parse(TTL_FILE_OLD, format='turtle')
    gNew = Graph().parse(TTL_FILE_NEW, format='turtle')

    gDiff = gNew-gOld
    gDiff.serialize(destination=TTL_FILE_DIFF, format='turtle')


def getLatestTTLVersion():
    old_version = getVersion(-2)
    getTTL(old_version, TTL_FILE_OLD)
    latest_version = getVersion(-1)
    getTTL(latest_version, TTL_FILE_NEW)

    # Create TTL Diff file 
    create_diff_ttl()
    return

    
