#!/usr/bin/env python

import boto3
import os
import structlog
from blackfynn import Blackfynn
from base import DynamoDBClient, SSMClient
from botocore.exceptions import ClientError
import logging

log = logging.getLogger(__name__)

class Configs(object):
    
    
    environment_name = os.environ.get("ENVIRONMENT_NAME", "test")
    service_name = os.environ.get("SERVICE_NAME", "sparc-tools")

    # Blackfynn client configurations
    working_directory = None
    session = None
    ssm = None
    ssm_path = None

    blackfynn_log_bind = {
        "service_name": service_name,
        "environment_name": environment_name,
    }

    # SPARC Tools configurations
    base_url = "https://cassava.ucsd.edu/sparc/archive/exports/"
    json_metadata_expired = "{}/expired_metadata.json".format(working_directory)
    json_metadata_full = "{}/full_metadata.json".format(working_directory)
    json_metadata_new = "{}/new_metadata.json".format(working_directory)
    json_metadata_file = "{}/metadata.json".format(working_directory)
    ttl_file_old = '{}/curation-export-old.ttl'.format(working_directory)
    ttl_file_new = '{}/curation-export-new.ttl'.format(working_directory)

    def __init__(self, env):
        # AWS configurations
        aws_key = os.environ.get("AWS_PUBLIC_KEY", "aws-key")
        aws_secret = os.environ.get("AWS_SECRET_KEY", "aws-secret")
        dynamodb_endpoint = os.environ.get("DYNAMODB_ENDPOINT", "http://localhost:4569")
        table_arn = os.environ.get("SPARC_METADATA_DYNAMODB_TABLE_ARN", "sparc-metadata-table-arn")
        table_id = os.environ.get("SPARC_METADATA_DYNAMODB_TABLE_ID", "prod-sparc-metadata-table-use1")
        aws_region = os.environ.get("AWS_REGION_NAME", "us-east-1")
        table_partition_key = "datasetId"
        table_sort_key = "recordId"

        log.info('GETTING CONFIG FOR: {}'.format(env))

        if env == "dev":

            log.info('SETTING UP CONFIG FOR: {}'.format(env))

            self.env = "dev"
            self.last_updated = None
            self.working_directory = os.getcwd()
            blackfynn_host="https://api.blackfynn.net"
            blackfynn_api_token = os.environ.get("BLACKFYNN_API_TOKEN")
            blackfynn_api_secret = os.environ.get("BLACKFYNN_API_SECRET")
            
            self.ssm_path = "/{}/sparc-tools/".format(env)
            self.ssm = SSMClient(aws_region, "dev", self.ssm_path, "http://localhost:4583")

            # self.session = boto3.Session(region_name = aws_region)
            self.bf = Blackfynn( api_token=blackfynn_api_token, 
                        api_secret=blackfynn_api_secret, 
                        host=blackfynn_host)

            self.db_client =  DynamoDBClient(aws_region, env, table_partition_key, "http://localhost:4569",table_id, table_sort_key)

        elif env == "prod":

            log.info('SETTING UP CONFIG FOR: {}'.format(env))

            self.env = "prod"
            self.last_updated = None
            self.working_directory = os.getcwd()
            blackfynn_host="https://api.blackfynn.io"
            blackfynn_api_token = os.environ.get("BLACKFYNN_API_TOKEN")
            blackfynn_api_secret = os.environ.get("BLACKFYNN_API_SECRET")
            
            self.ssm_path = "/{}/sparc-tools/".format(env)
            self.ssm = SSMClient(aws_region, "dev", self.ssm_path, "http://localhost:4583")

            # self.session = boto3.Session(region_name = aws_region)
            self.bf = Blackfynn( api_token=blackfynn_api_token, 
                        api_secret=blackfynn_api_secret, 
                        host=blackfynn_host)

            self.db_client =  DynamoDBClient(aws_region, env, table_partition_key, "http://localhost:4569",table_id, table_sort_key)




            # self.env = "prod"
            # self.working_directory = "/tmp"
            # self.last_updated = ssm.get_parameter(Name=ssm_path + "last_updated")
            # blackfynn_host="https://api.blackfynn.io"
            # blackfynn_api_token = ssm.get_parameter(Name=ssm_path + "blackfynn-api-key", WithDecryption=True)["Parameter"]["Value"]
            # blackfynn_api_secret = ssm.get_parameter(Name=ssm_path + "blackfynn-api-secret", WithDecryption=True)["Parameter"]["Value"]
            # self.bf = Blackfynn( api_token=blackfynn_api_token, 
            #             api_secret=blackfynn_api_secret, 
            #             host=blackfynn_host)
            # self.session = boto3.Session(region_name=aws_region)
            # self.ssm = self.session.client("ssm", aws_region, env, table_partition_key, dynamodb_endpoint)
            # self.ssm_path = "/{}/sparc-tools/".format(environment_name)
            # self.db_client = DynamoDBClient(self, aws_region, env, table_partition_key, dynamodb_endpoint,table_id,table_sort_key)

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
