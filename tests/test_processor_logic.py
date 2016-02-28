# -*- coding: utf-8 -*-
"""Basic unit tests."""

import inspect
import json
from mock import Mock
import os
import sys
import uuid

import pytest

# Add the lambda directory to the python library search path
lambda_dir = os.path.join(
    os.path.dirname(inspect.getfile(inspect.currentframe())), '..')
sys.path.append(lambda_dir)

import lambdautils.utils as utils


def test_get_secret(boto3_resource, boto3_client, monkeypatch):
    """Gets a secret from DynamoDB."""
    # Call to the DynamoDB client to retrieve the encrypted secret
    monkeypatch.setattr("boto3.resource", boto3_resource)
    monkeypatch.setattr("boto3.client", boto3_client)
    utils.get_secret("sample_secret")
    boto3_client("dynamodb").get_item.assert_called_with(
        TableName=utils.SECRETS_TABLE_NAME,
        Key={"id": {"S": "sample_secret"}})

    # Call to the KMS client to decrypt the secret
    boto3_client('kms').decrypt.assert_called_with(CiphertextBlob="encrypted")


def test_get_state(boto3_resource, monkeypatch):
    """Gets a state value from DynamoDB."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    utils.get_state("sample_state_key")
    boto3_resource("dynamodb").Table().get_item.assert_called_with(
        Key={"id": "sample_state_key"})


def test_set_state(boto3_resource, monkeypatch):
    """Tests setting a state value."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    utils.set_state("sample_state_key", "sample_state_value")
    boto3_resource("dynamodb").Table().put_item.assert_called_with(
        Item={"id": "sample_state_key", "value": "sample_state_value"})


def test_sentry_monitor(raven_client, context, monkeypatch):
    """Tests the sentry_monitor decorator."""

    monkeypatch.setattr("raven.Client", Mock(return_value=raven_client))

    @utils.sentry_monitor(dsn=str(uuid.uuid4()))
    def lambda_handler(event, context):
        pass

    lambda_handler(None, context)
    raven_client.captureException.assert_not_called()


def test_sentry_monitor_exception(raven_client, context, monkeypatch):
    """Tests the sentry_monitor decorator."""
    monkeypatch.setattr("raven.Client", Mock(return_value=raven_client))

    @utils.sentry_monitor(dsn=str(uuid.uuid4()))
    def lambda_handler(event, context):
        raise KeyError

    with pytest.raises(KeyError):
        lambda_handler(None, context)

    assert raven_client.captureException.call_count == 1


def test_context_dict(context):
    """Tests utility context_dict."""
    d = utils.context_dict(context)
    assert len(d) == 8
    assert d["function_name"] == context.function_name


def test_send_to_kinesis_stream(search_events, boto3_client, monkeypatch):
    """Tests sending events to a Kinesis stream."""
    monkeypatch.setattr("boto3.client", boto3_client)
    utils.send_to_kinesis_stream(search_events, "dummy_stream")
    boto3_client("kinesis").put_records.call_count == 1


def test_send_to_delivery_stream(search_events, boto3_client, monkeypatch):
    """Tests sending events to a Firehose delivery stream."""
    monkeypatch.setattr("boto3.client", boto3_client)
    utils.send_to_delivery_stream(search_events, "dummy_stream")
    boto3_client("firehose").put_record_batch.call_count == 1


def test_unpack_kinesis_event(kinesis_event):
    """Extracts json-serialized events from a Kinesis events."""
    events = utils.unpack_kinesis_event(kinesis_event,
                                        post_processor=json.loads)
    # There should be one event per kinesis record
    assert len(events) == len(kinesis_event["Records"])
