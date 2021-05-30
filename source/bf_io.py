from pennsieve import Pennsieve, ModelProperty, LinkedModelProperty
from pennsieve.base import UnauthorizedException
from pennsieve.models import ModelPropertyEnumType, BaseCollection
from base import MODEL_NAMES, SPARC_DATASET_ID
from requests.exceptions import HTTPError
import logging, math
from datetime import datetime as DT

log = logging.getLogger(__name__)

### pennsieve platform I/O:
class pennsieveException(Exception):
    'Represents Exceptions raised by the API'
    pass

def authorized(bf, dsId):
    '''check if user is authorized as a manager'''
    api = bf._api.datasets
    try:
        role = api._get(api._uri('/{dsId}/role', dsId=dsId)).get('role')
    except UnauthorizedException:
        return False
    except:
        return False
    return str(role)

def get_create_hash_ds(bf):
    """ Create or get dataset used to track updates
    """
    try:
        ds = bf.get_dataset('sparc_curation_sync')
    except:
        log.warning('Failed to get dataset --> Creating dataset: {}"'.format('sparc_curation_sync'))
        ds = bf.create_dataset('sparc_curation_sync')

    # Clear dataset model in case the structure has changed
#     clear_model(bf,ds,'dataset')

    try:
        model = ds.get_model('dataset')
    except:
        ds.create_model('dataset', 'dataset', schema=[
            ModelProperty('ds_id', 'ds_id', title=True),
            ModelProperty('protocol', 'Protocol' ),
            ModelProperty('term', 'Term'),
            ModelProperty('researcher', 'Researcher'),
            ModelProperty('subject', 'Subject'),
            ModelProperty('sample', 'Sample'),
            ModelProperty('award', 'Award'),
            ModelProperty('summary', 'Summary'),
            ModelProperty('tag', 'Tags')])
        
    return ds

def add_file_to_record(bf, ds, record_id, file_id):
    log.info("Linking file_id: {} to record_id: {}".format(file_id, record_id))
    host = "{}/".format(bf._api.settings.api_host)

    payload = {
      "targets": [
        {
          "linkTarget": {
            "ConceptInstance": {
              "id": record_id
            }
          },
          "relationshipData": [
          ],
          "direction": "FromTarget",
          "relationshipType": "belongs_to"
        }
      ],
      "externalId": file_id
    }

    try:
        response = bf._api._post(host = host,
                                 base="models/v1/",
                                 endpoint="datasets/{}/proxy/package/instances".format(ds.id),
                                 json=payload)
    except:
        raise
        log.warning("Something went wrong with adding a file/folder to a record on platform.")
        return None




## Get is_locked
def get_publication_status(bf, ds_id):

    org_int_id = bf.context.int_id
    host = "{}/".format(bf._api.settings.api_host)

    response = None
    try:
        response =  bf._api._get(host = host,
        base="",
        endpoint="datasets/{}".format(ds_id))
    except:
        log.error("Cannot get dataset information")
        raise

    return response['publication']['status']


### Conduct search 
def search_for_records(bf, ds, model_name, filters):
    """ Returns JSON representation of record
    """

    org_int_id = bf.context.int_id
    host = "{}/".format(bf._api.settings.api_host)

    payload = { "model": model_name,
        "datasets": [ds.int_id],
        "filters": filters}

    try:
        response = bf._api._post(host = host, 
            base="models/v2/", 
            endpoint="organizations/{}/search/records".format(org_int_id),
            json=payload)
    except:
        raise
        log.warning("Something went wrong with searching on platform.")
        return None

    rec = None
    if response['records']:
        records = response['records']
        if len(records) > 1:
            raise(Exception('More than one search result, this is unlikely to happen'))
        
        rec = records[0]

        log.info("Found Record: {}-{}".format(model_name, rec['id']))
    else:
        log.info("COULD NOT FIND RECORD: {}".format(filters))

    return rec

### Create many links
def create_links(bf, dataset, model_id, record_id, payload):

        dataset_id = dataset.id
        json = {"data": payload}

        resp = bf._api._post(
            host = "{}/".format(bf._api.settings.api_host),
            base = "models/v1/",
            endpoint = "datasets/{}/concepts/{}/instances/{}/linked/batch".format(dataset_id,model_id,record_id),
            json=json
        )

        results = resp["data"]

        return results

### Get dataset by ID/Name or create dataset with name that is ID
def get_create_dataset(bf, dsId):
    try:
        ds = bf.get_dataset(dsId)
    except:
        log.warning('Failed to get dataset --> Creating dataset: {}"'.format(dsId))
        ds = bf.create_dataset(dsId)

    return ds

def clear_dataset(bf, dataset):
    '''
    DANGER! Deletes all records of type:
    - protocol
    - researcher
    - sample
    - subject
    - summary
    - term
    - human_subject
    - animal_subject

    Also removes models.

    '''

    models = dataset.models().values()
    for m in models:
        if m.type not in MODEL_NAMES:
            continue
        elif m.count > 0:
            recs = m.get_all(limit = m.count)
            m.delete_records(*recs)
        m.delete()
            
    log.info("Cleared dataset '{}'".format(dataset.name))

def clear_model(bf, ds, model_name):
    try:
        model = ds.get_model(model_name)
    except:
        print('Model {} does not exist in {}'.format(model_name, ds))
        return

    n = 100
    nr_batches = math.floor(model.count/n )
    if nr_batches > 1:
        for i in range(0, nr_batches):
            recs = model.get_all(limit = n)
            model.delete_records(*recs)

    recs = model.get_all(limit = n)
    model.delete_records(*recs)

    model.delete()
    
def get_create_model(bf, ds, name, displayName, schema=None, linked=None):
    '''create a model if it doesn't exist,
    or retrieve it and update its schema properties'''
    if schema is None:
        schema = []
    if linked is None:
        linked = []

    # Try to get model or create model if not exist.
    model = None
    try:
        model = ds.get_model(name)
        if schema:
            raise(Exception("Trying to update schema of existing model"))
    except HTTPError:
        if schema:
            model = ds.create_model(name, displayName, schema=schema)
        else:
            raise(Exception("Unsuccessful in creating new model --> no schema"))

    # Check if links contain linked properties that don't exist and add if the case.
    newLinks = [l for l in linked if l.name not in model.linked]
    if newLinks:
        log.info("Has new Property Links for: {}".format(name))
        model.add_linked_properties(newLinks)

    return model

#%% [markdown]
### Update the SPARC Dashboard
# This adds an entry to the sparc dashboard for this update run
#%%
def update_sparc_dashboard(bf):
    sparc_ds = get_create_dataset(bf, SPARC_DATASET_ID)
    model = sparc_ds.get_model('Update_Run')
    model.create_record({'name':'TTL Update', 'status': DT.now()})