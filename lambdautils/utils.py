# -*- coding: utf-8 -*-
"""Utilities for Lambda functions deployed using humilis."""


import boto3
import json
import uuid

# Tell humilis to pre-process the file with Jinja2
# preprocessor:jinja2

if len("{{_env.stage}}") > 0:
    SECRETS_TABLE_NAME = "{{_env.name}}-{{_env.stage}}-secrets"
    STATE_TABLE_NAME = "{{_env.name}}-{{_layer.name}}-{{_env.stage}}-state"
else:
    SECRETS_TABLE_NAME = "{{_env.name}}-secrets"
    STATE_TABLE_NAME = "{{_env.name}}-{{_layer.name}}-state"


def get_secret(key):
    """Retrieves a secret from the secrets vault."""
    # Get the encrypted secret from DynamoDB
    client = boto3.client('dynamodb')
    encrypted = client.get_item(
        TableName=SECRETS_TABLE_NAME,
        Key={'id': {'S': key}}).get('Item', {}).get('value', {}).get('B')

    if encrypted is None:
        return

    # Decrypt using KMS
    client = boto3.client('kms')
    return client.decrypt(CiphertextBlob=encrypted)['Plaintext'].decode()


def get_state(key):
    """Gets a state value from the state table."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(STATE_TABLE_NAME)
    print("Getting key '{}' from table '{}'".format(key, STATE_TABLE_NAME))
    return table.get_item(Key={"id": key}).get("Item", {}).get("value")


def set_state(key, value):
    """Sets a state value."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(STATE_TABLE_NAME)
    return table.put_item(Item={"id": key, 'value': value})


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


def send_to_kinesis_stream(events, stream_name):
    """Sends events to a Kinesis stream."""
    records = []
    for event in events:
        if not isinstance(event, str):
            event = json.dumps(event)
        record = {"Data": event,
                  "PartitionKey": str(uuid.uuid4())}
        records.append(record)

    kinesis = boto3.client("kinesis")
    resp = kinesis.put_records(StreamName=stream_name, Records=records)
    return resp
