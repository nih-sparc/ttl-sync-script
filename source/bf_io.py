from blackfynn import Blackfynn, ModelProperty, LinkedModelProperty
from blackfynn.base import UnauthorizedException
from blackfynn.models import ModelPropertyEnumType, BaseCollection
from base import MODEL_NAMES, SPARC_DATASET_ID
from requests.exceptions import HTTPError
import logging
from datetime import datetime as DT

log = logging.getLogger(__name__)

### Blackfynn platform I/O:
class BlackfynnException(Exception):
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