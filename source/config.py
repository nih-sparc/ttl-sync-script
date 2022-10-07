#!/usr/bin/env python

import os
import structlog
from pennsieve import Pennsieve
import logging

log = logging.getLogger(__name__)

class Configs(object):
    
    environment_name = os.environ.get("ENVIRONMENT_NAME", "test")
    service_name = os.environ.get("SERVICE_NAME", "sparc-tools")

    # Pennsieve client configurations
    working_directory = None
    session = None
    ssm = None
    ssm_path = None
    db_client = None

    blackfynn_log_bind = {
        "service_name": service_name,
        "environment_name": environment_name,
    }

    # SPARC Tools configurations
    working_directory = '/tmp'
    base_url = "https://cassava.ucsd.edu/sparc/exports/"
    json_metadata_expired = "{}/expired_metadata.json".format(working_directory)
    json_metadata_full = "{}/full_metadata.json".format(working_directory)
    json_metadata_new = "{}/new_metadata.json".format(working_directory)
    json_metadata_file = "{}/metadata.json".format(working_directory)
    ttl_file_old = '{}/curation-export-old.ttl'.format(working_directory)
    ttl_file_new = '{}/curation-export-new.ttl'.format(working_directory)
    json_cache_file = '{}/curation-json-cache.json'.format(working_directory)
    ttl_resume_file = '{}/ttl_update_resume.json'.format(working_directory)

    def __init__(self, env):

        log.info('GETTING CONFIG FOR: {}'.format(env))

        if env == "dev":

            log.info('SETTING UP CONFIG FOR: {}'.format(env))

            self.env = "dev"
            self.last_updated = None
            blackfynn_host="https://api.pennsieve.net"
            blackfynn_api_token = os.environ.get("PENNSIEVE_API_TOKEN")
            blackfynn_api_secret = os.environ.get("PENNSIEVE_API_SECRET")

            self.bf = Pennsieve( api_token=blackfynn_api_token,
                        api_secret=blackfynn_api_secret, 
                        host=blackfynn_host)

        elif env == "prod":

            log.info('SETTING UP CONFIG FOR: {}'.format(env))

            self.env = "prod"
            self.last_updated = None
            blackfynn_host="https://api.pennsieve.io"
            blackfynn_api_token = os.environ.get("PENNSIEVE_API_TOKEN")
            blackfynn_api_secret = os.environ.get("PENNSIEVE_API_SECRET")
            
            self.bf = Pennsieve( api_token=blackfynn_api_token,
                        api_secret=blackfynn_api_secret, 
                        host=blackfynn_host)

        else:
            raise(Exception('Incorrect input argument'))

class StructLog(object):
    def rewrite_event_to_message(self, name, event_dict):
        """
        Rewrite the default structlog `event` to a `message`.
        """
        event = event_dict.pop("event", None)
        if event is not None:
            event_dict["message"] = event
        return event_dict

    def add_log_level(self, name, event_dict):
        event_dict["log_level"] = name.upper()
        return event_dict

    structlog.configure(
        processors=[
            rewrite_event_to_message,
            add_log_level,
            structlog.processors.format_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )
    log = structlog.get_logger()
