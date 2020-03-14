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
def getCreateDataset(bf, dsId):
    try:
        ds = bf.get_dataset(dsId)
    except:
        log.warning('Failed to get dataset --> Creating dataset: {}"'.format(dsId))
        ds = bf.create_dataset(dsId)

    return ds

def clearDataset(bf, dataset):
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

def getModel(bf, ds, name, displayName, schema=None, linked=None):
    '''create a model if it doesn't exist,
    or retrieve it and update its schema properties'''
    if schema is None:
        schema = []
    if linked is None:
        linked = []
    try:
        model = ds.get_model(name)
        try:
            for s in schema:
                s.id = model.schema[s.name].id
            newLinks = [l for l in linked if l.name not in model.linked]
            model.schema = {s.name: s for s in schema}
            model.update()
            if newLinks:
                try:
                    model.add_linked_properties(newLinks)
                except Exception as e:
                    log.info("Error adding linked properties '{}' to dataset '{}': {}".format(newLinks, ds.name, e))
        except Exception as e:
            log.info("Error updating model '{}' with schema '{}' to dataset '{}': {}".format(model, schema, ds.name, e))
    except HTTPError:
        #log.info("model '{}' not found in dataset '{}': trying to create it".format(name, ds.name))
        try:
            model = ds.create_model(name, displayName, schema=schema)
            if linked:
                model.add_linked_properties(linked)
        except Exception as e:
            log.info("Error creating model '{}' with schema '{}' in dataset '{}': {}".format(model, schema, ds.name, e))
    return model

#%% [markdown]
### Update the SPARC Dashboard
# This adds an entry to the sparc dashboard for this update run
#%%
def update_sparc_dashboard():
    sparc_ds = getCreateDataset(SPARC_DATASET_ID)
    model = sparc_ds.get_model('Update_Run')
    model.create_record({'name':'TTL Update', 'status': DT.now()})