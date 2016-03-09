# -*- coding: utf-8 -*-
"""Utilities for Lambda functions deployed using humilis."""

import base64
import json
import logging
import traceback
import uuid

import boto3
from botocore.exceptions import ClientError
import raven

# Tell humilis to pre-process the file with Jinja2
# preprocessor:jinja2

if len("{{_env.stage}}") > 0:
    SECRETS_TABLE_NAME = "{{_env.name}}-{{_env.stage}}-secrets"
    STATE_TABLE_NAME = "{{_env.name}}-{{_layer.name}}-{{_env.stage}}-state"
else:
    SECRETS_TABLE_NAME = "{{_env.name}}-secrets"
    STATE_TABLE_NAME = "{{_env.name}}-{{_layer.name}}-state"


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secret(key):
    """Retrieves a secret from the secrets vault."""
    # Get the encrypted secret from DynamoDB
    client = boto3.client('dynamodb')
    try:
        encrypted = client.get_item(
            TableName=SECRETS_TABLE_NAME,
            Key={'id': {'S': key}}).get('Item', {}).get('value', {}).get('B')
    except ClientError:
        print("DynamoDB error when retrieving secret '{}'".format(key))
        traceback.print_exc()
        return

    if encrypted is None:
        return

    # Decrypt using KMS
    client = boto3.client('kms')
    try:
        return client.decrypt(CiphertextBlob=encrypted)['Plaintext'].decode()
    except ClientError:
        print("KMS error when trying to decrypt secret")
        traceback.print_exc()
        return


def get_state(key):
    """Gets a state value from the state table."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(STATE_TABLE_NAME)
    print("Getting key '{}' from table '{}'".format(key, STATE_TABLE_NAME))
    try:
        return table.get_item(Key={"id": key}).get("Item", {}).get("value")
    except ClientError:
        print("DynamoDB error when retrieving key '{}' from table '{}'".format(
            key, STATE_TABLE_NAME))
        traceback.print_exc()
        return


def set_state(key, value):
    """Sets a state value."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(STATE_TABLE_NAME)
    return table.put_item(Item={"id": key, "value": value})


def send_to_delivery_stream(events, stream_name):
    """Sends a list of events to a Firehose delivery stream."""
    records = []
    for event in events:
        if not isinstance(event, str):
            # csv events already have a newline
            event = json.dumps(event) + "\n"
        records.append({"Data": event})
    firehose = boto3.client("firehose")
    print("Delivering {} records to Firehose stream '{}'".format(
        len(records), stream_name))
    resp = firehose.put_record_batch(
        DeliveryStreamName=stream_name,
        Records=records)
    return resp


def send_to_kinesis_stream(events, stream_name, partition_key=None):
    """Sends events to a Kinesis stream."""
    records = []
    for event in events:
        if not isinstance(event, str):
            event = json.dumps(event)
        if partition_key is not None:
            partition_key_value = event.get(partition_key)
        else:
            partition_key_value = str(uuid.uuid4())

        record = {"Data": event,
                  "PartitionKey": partition_key_value}
        records.append(record)

    kinesis = boto3.client("kinesis")
    resp = kinesis.put_records(StreamName=stream_name, Records=records)
    return resp


def sentry_monitor(dsn=None):
    """A decorator that adds Sentry monitoring to a Lambda handler."""
    if dsn is None:
        dsn = get_secret("sentry.dsn")

    if dsn is None:
        logger.warning("Unable to retrieve sentry DSN")
    elif not hasattr(sentry_monitor, "clients"):
        client = raven.Client(dsn)
        clients = {dsn: client}
        setattr(sentry_monitor, "clients", clients)
    else:
        clients = getattr(sentry_monitor, "clients")
        if dsn not in clients:
            clients[dsn] = raven.Client(dsn)
        client = clients[dsn]

    def decorator(func):
        def wrapper(event, context):
            if dsn is not None:
                client.user_context(context_dict(context))
                try:
                    return func(event, context)
                except:
                    client.captureException()
                    raise
            else:
                return func(event, context)
        return wrapper

    return decorator


def context_dict(context):
    """Converst the Lambda context object to a dict."""
    return {
        "function_name": context.function_name,
        "function_version": context.function_version,
        "invoked_function_arn": context.invoked_function_arn,
        "memory_limit_in_mb": context.memory_limit_in_mb,
        "aws_request_id": context.aws_request_id,
        "log_group_name": context.log_group_name,
        "cognito_identity_id": context.identity.cognito_identity_id,
        "cognito_identity_pool_id": context.identity.cognito_identity_pool_id}


def unpack_kinesis_event(kinesis_event, post_processor=None):
    """Extracts events (a list of dicts) from a Kinesis event."""
    records = kinesis_event['Records']
    events = []
    for rec in records:
        payload = base64.decodestring(rec['kinesis']['data']).decode()
        if post_processor is not None:
            payload = post_processor(payload)

        events.append(payload)

    return events
