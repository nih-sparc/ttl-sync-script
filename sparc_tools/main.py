#!/usr/bin/env python3
'''
Download the latest SPARC TTL files,
Convert to JSON,
Then update all datasets.
'''
import logging
import sys

from base import (
    TTL_FILE_NEW,
    TTL_FILE_OLD
)
import expired_metadata
import metadata_versions
import new_metadata
import parse_json
from config import Configs


logging.basicConfig(format="%(filename)s:%(lineno)d:\t%(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
cfg = Configs()


def main(event = None, context = None):

    lastUpdated = metadata_versions.getLastUpdated()
    latestVersion = metadata_versions.latestVersion()

    log.info(latestVersion)
    if len(sys.argv) > 1 and sys.argv[1] == "retry":
        log.info('Running parse_json.updateAll(reset=True)')
        parse_json.updateAll(reset=True)
    elif len(sys.argv) > 1 and sys.argv[1] == "retryDiff":
        log.info('Running parse_json.updateAll(reset=False)')
        parse_json.updateAll()
    elif len(sys.argv) > 1 and sys.argv[1] == "fullJsonOnly":
        log.info('New metadata version: {} old version: {}'.format(latestVersion, lastUpdated))
        metadata_versions.getTTL(latestVersion, TTL_FILE_NEW)

        log.info('Metadata file downloaded.')
        new_metadata.buildJson('full')

        log.info('json File build')
    elif len(sys.argv) > 1 and sys.argv[1] == "diffJsonOnly":
        log.info('New metadata version: {} old version: {}'.format(latestVersion, lastUpdated))
        metadata_versions.getTTL(lastUpdated, TTL_FILE_OLD)
        metadata_versions.getTTL(latestVersion, TTL_FILE_NEW)
        log.info('Metadata files downloaded.')

        expired_metadata.buildJson()
        log.info('expired Metadata file build.')
        new_metadata.buildJson('diff')
    elif (len(sys.argv) > 1 and sys.argv[1] == "forceOverwrite") or lastUpdated is None or lastUpdated == "0":
        # Full update to latest version
        log.info('New metadata version: {}'.format(latestVersion, lastUpdated))
        metadata_versions.getTTL(latestVersion, TTL_FILE_NEW)

        log.info('Metadata file downloaded.')
        new_metadata.buildJson('full')

        log.info('Running parse_json.updateAll')
        parse_json.updateAll(reset=True)
    else:
        # Diff update from last updated version to latest version
        if latestVersion <= lastUpdated:
            log.info("No new metadata is available. Quitting...")
            sys.exit()

        log.info('New metadata version: {} old version: {}'.format(latestVersion, lastUpdated))
        metadata_versions.getTTL(lastUpdated, TTL_FILE_OLD)
        metadata_versions.getTTL(latestVersion, TTL_FILE_NEW)
        log.info('Metadata files downloaded.')

        expired_metadata.buildJson()
        log.info('expired Metadata file build.')
        new_metadata.buildJson('diff')
        log.info('new Metadata file build.')
        # fallback to a full reset/update for any datasets that failed to update:
        failedDatasets = parse_json.updateAll()
        parse_json.update(failedDatasets, reset=True)

    if not cfg.dry_run:
        metadata_versions.setLastUpdated(latestVersion)
        parse_json.update_sparc_dataset()

if __name__ == '__main__':
    main()
