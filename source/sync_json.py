### Removing data: Delete specific records from dataset
def deleteData(ds, models, recordCache, node):
    '''
    Delete records and/or record properties from a dataset
    '''
    if node['expired']:
        log.info('Untracking dataset')
        for k in recordCache:
            recordCache[k].clear()
        return
    for modelName, modelNode in node['records'].items():
        model = models[modelName]
        oldRecs = []
        for identifier, recNode in modelNode.items():
            try:
                recId = recordCache[modelName][identifier]
            except KeyError:
                log.warning('Tried to delete from a nonexistent record')
                continue
            if recNode['expired']:
                log.debug('Deleting record {}/{}'.format( modelName, identifier))
                oldRecs.append(identifier)
                continue
            record = model.get(recId)
            if recNode['values'] or recNode['arrayValues']:
                log.debug('Deleting properties of record {}'.format(record))
                removeProperties(ds, record, recordCache, recNode['values'], recNode['arrayValues'])
                try:
                    removeRecords(model, recordCache, *oldRecs)
                except:
                    log.info("Error trying to delete record {}".format(record))

def removeRecords(model, recordCache, *recordNames):
    'Remove record(s), but only if they exist both in the cache and on the platform'
    recIds = []
    for identifier in recordNames:
        try:
            recId = recordCache[model.type].pop(identifier)
        except KeyError:
            log.warning("removeRecords: record '{}/{}' not found in cache".format(model.type, identifier))
            continue
        else:
            recIds.append(recId)
    try:
        print("deleting '{}' record {}".format( model.type, str(recIds)))
        model.delete_records(*recIds) # will print error message if a record doesn't exist
    except Exception as e:
        log.error("Failed to delete '{}' record(s): {}".format( model.type, str(recIds)))
        raise BlackfynnException(e)

def removeProperties(ds, record, recordCache, values, arrayValues):
    'Remove properties, linked properties and relationships on the platform'
    model = record.model
    ignoreProps = {
        'protocol': (),
        'researcher': (),
        'sample': ('hasDigitalArtifactThatIsAboutItHash'),
        'subject': ('localIdAlt'),
        'summary': (),
        'term': ('deprecated')}
    relnTypes = {
        'http://purl.obolibrary.org/obo/IAO_0000136': 'is-about',
        'protocolEmploysTechnique': 'protocol-employs-technique',
        'involvesAnatomicalRegion': 'involves-anatomical-region'}

    for v in values:
        if v in ignoreProps[model.type]:
            continue
        elif v in model.linked:
            try:
                record.delete_linked_value(v)
            except:
                log.info("Error trying to delete record linked value {}".format(v))
        else:
            record._set_value(v, None)
        try:
            print("deleting property '{}' from record {}".format(v, record))
            record.update()
        except Exception as e:
            log.error("Failed to remove property '{}' from record {}".format(v, record))
            raise BlackfynnException(e)

    for prop, values in arrayValues.items():
        if prop in ignoreProps[model.type]:
            continue
        elif prop in relnTypes:
            rt = ds.get_relationship(relnTypes[prop])
            relationships = rt.get_all()
            for v in values:
                try:
                    targetId = recordCache['term'][v]
                except KeyError:
                    continue
                for r in relationships:
                    if r.source == record.id and r.destination == targetId:
                        r.delete()
        else:
            vals = record.get(prop)
            if vals is None:
                continue
            array = [x for x in vals if x not in values]
            if array == []:
                array = None
            try:
                print("editing property '{}' from record {}".format(prop, record))
                record.set(prop, array)
            except Exception as e:
                log.error("Failed to edit property '{}' of record '{}'".format(prop, record))
                raise BlackfynnException(e)

def addRelationships(ds, recordCache, model, record, relationships, file):
    'Add relationships to a record'
    terms = ds.get_bf_model(ds, 'term')
    for name, values in relationships.items():
        if name in ('is-about', 'involves-anatomical-region', 'protocol-employs-technique'):
            try:
                rt = ds.create_relationship_type(name, description='',
                    source=model.id, destination=terms.id, display_name=name.replace('-', ' ').title())
            except:
                rt = ds.get_relationship(name)
            targets = []
            for v in values:
                if v in recordCache['term']:
                    targets.append(terms.get(recordCache['term'][v]))
                elif not v.isdigit():
                    target = terms.create_record({'label': v})
                    log.debug('addRelationships: created new record {}'.format(target))
                    recordCache['term'][v] = target.id
                    targets.append(target)
            try:
                file.append("adding '{}' relationship to record '{}'".format(rt.type, record))
                record.relate_to(targets, relationship_type=rt)
            except Exception as e:
                log.error("Failed to add '{}' relationship to record '{}'".format(rt.type, record))
                raise BlackfynnException(e)

# def sync_datasets(cfg, method = 'full'):
#     log.info('===========================')
#     log.info('== Deleting old metadata ==')
#     log.info('===========================')
#     log.info('')

#     delete_start_time = time()

#     ## Delete the old data in existing dataset for specific models
#     for dsId, node in oldJson.items():
#         log.info('Current dataset: {}'.format(dsId))

#         # Get Dataset, or Create dataset with Name=dsId if it does not exist.
#         ds = getCreateDataset(dsId)

#         # If reset, then clear out all records. Otherwise, only clear out records that were 
#         # added through this process
#         if method == 'full':
#             clearDataset(cfg.bf, ds)
#             recordCache = {m: {} for m in MODEL_NAMES}
#         else:
#             recordCache = cfg.db_client.buildCache(dsId)

#         models = {k: v for k, v in ds.models().items() if k in MODEL_NAMES}
#         try:
#             deleteData(ds, models, recordCache, node)
#         except BlackfynnException:
#             log.error("Dataset {} failed to update".format(dsId))
#             failedDatasets.append(dsId)
#             continue
#         finally:
#             cfg.db_client.writeCache(dsId, recordCache)

#     recordCache = cfg.db_client.buildCache(dsId)

#     duration = int((time() - delete_start_time) * 1000)
#     log.info("Deleted old metadata in {} milliseconds".format(duration))

def updateRecord(ds, recordCache, model, identifier, values, links=None, relationships=None):
    '''
    Create or update a record with the given properties/relationships
    model: Model object
    identifier: record identifier
    values: record's schema values
    links: linked values (structured {name: identifier})
    relationships: relationships(structured {name: [identifiers]})
    '''
    try:
        recId = recordCache[model.type][identifier]
    except KeyError:
        try:
            rec = model.create_record(values)
            log.info('Created new record: {}'.format(rec))
            recordCache[model.type][identifier] = rec.id
        except Exception as e:
            log.error("Failed to create record with values {}".format(values))
            return None
    else:
        rec = model.get(recId)
        log.debug('Retrieved record from cache: {}'.format(rec))
        for prop, value in rec.values.items():
            if prop not in values or values[prop] in (None, '', '(no label)'):
                values[prop] = value
            elif isinstance(value, list):
                values[prop] = list(set(values[prop]).union(set(value)))
        try:
            rec._set_values(values)
            log.info('updating record')
            rec.update()
        except Exception as e:
            log.error("Failed to update values of record {}".format(rec))
            raise BlackfynnException(e)

    # if links:
    #     addLinks(ds, recordCache, model, rec, links)
    # if relationships:
    #     addRelationships(ds, recordCache, model, rec, relationships)
    return rec