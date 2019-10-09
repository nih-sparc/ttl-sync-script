#!/usr/bin/env python

import boto3
import os
import structlog

from botocore.exceptions import ClientError

class Configs(object):
    # Blackfynn configurations
    environment_name = os.environ.get("ENVIRONMENT_NAME", "test")
    service_name = os.environ.get("SERVICE_NAME", "sparc-tools")
    dry_run = os.environ.get("DRY_RUN")
    if dry_run == "False":
        dry_run = False
    else:
        dry_run = True
    blackfynn_log_bind = {
        "service_name": service_name,
        "environment_name": environment_name,
    }

    # AWS configurations
    aws_key = os.environ.get("AWS_PUBLIC_KEY", "aws-key")
    aws_secret = os.environ.get("AWS_SECRET_KEY", "aws-secret")
    aws_region = os.environ.get("AWS_REGION_NAME", "us-east-1")
    dynamodb_endpoint = os.environ.get("DYNAMODB_ENDPOINT", "http://localhost:8000")
    table_arn = os.environ.get("SPARC_METADATA_DYNAMODB_TABLE_ARN", "sparc-metadata-table-arn")
    table_id = os.environ.get("SPARC_METADATA_DYNAMODB_TABLE_ID", "prod-sparc-metadata-table-use1")
    table_partition_key = "datasetId"
    table_sort_key = "recordId"

    # Blackfynn client configurations
    if environment_name == "test":
        blackfynn_api_token = os.environ.get("BLACKFYNN_API_TOKEN")
        blackfynn_api_secret = os.environ.get("BLACKFYNN_API_SECRET")
        blackfynn_host="https://dev.blackfynn.io/"
        last_updated = None
        working_directory = os.getcwd()

    else:
        # Create ssm session to pull secrets

        session = boto3.Session(region_name=aws_region)
        ssm = session.client("ssm")
        ssm_path = "/{}/sparc-tools/".format(environment_name)

        # Pull configurations from SSM
        blackfynn_api_token = ssm.get_parameter(Name=ssm_path + "blackfynn-api-key", WithDecryption=True)["Parameter"]["Value"]
        blackfynn_api_secret = ssm.get_parameter(Name=ssm_path + "blackfynn-api-secret", WithDecryption=True)["Parameter"]["Value"]
        blackfynn_host="https://api.blackfynn.io/"
        last_updated = ssm.get_parameter(Name=ssm_path + "last_updated")
        #last_updated = "2019-07-23T09:59:50,233853"
        working_directory = "/tmp"

    # SPARC Tools configurations
    base_url = "https://cassava.ucsd.edu/sparc/archive/exports/"
    json_metadata_expired = "{}/expired_metadata.json".format(working_directory)
    json_metadata_full = "{}/full_metadata.json".format(working_directory)
    json_metadata_new = "{}/new_metadata.json".format(working_directory)
    json_metadata_file = "{}/metadata.json".format(working_directory)
    ttl_file_old = '{}/curation-export-old.ttl'.format(working_directory)
    ttl_file_new = '{}/curation-export-new.ttl'.format(working_directory)

class StructLog(object):
    def rewrite_event_to_message(logger, name, event_dict):
        """
        Rewrite the default structlog `event` to a `message`.
        """
        event = event_dict.pop("event", None)
        if event is not None:
            event_dict["message"] = event
        return event_dict

    def add_log_level(logger, name, event_dict):
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
