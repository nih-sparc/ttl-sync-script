'Download the two latest SPARC TTLs'
from shutil import copyfileobj

import boto3
from bs4 import BeautifulSoup
#from moto import mock_ssm
import requests
from requests.compat import quote, unquote

from base import SSMClient
# from config import Configs

from base import (
    TTL_FILE_NEW,
    TTL_FILE_OLD
)

BASE_URL = 'https://cassava.ucsd.edu/sparc/archive/exports/'
# cfg = Configs()
# ssm = SSMClient()

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

def getLatestTTLVersion():
    version = latestVersion()
    return getTTL(version, TTL_FILE_NEW)
