# -*- coding: utf-8 -*-
"""Basic unit tests."""

import inspect
import json
from mock import Mock
import os
import sys

import pytest

# Add the lambda directory to the python library search path
lambda_dir = os.path.join(
    os.path.dirname(inspect.getfile(inspect.currentframe())), '..')
sys.path.append(lambda_dir)

import lambdautils.utils


def test_get_secret(boto3_resource, boto3_client, monkeypatch):
    """Gets a secret from DynamoDB."""
    # Call to the DynamoDB client to retrieve the encrypted secret
    monkeypatch.setattr("boto3.resource", boto3_resource)
    monkeypatch.setattr("boto3.client", boto3_client)
    secret = lambdautils.utils.get_secret("sample_secret",
                                          environment="dummyenv",
                                          stage="dummystage")
    assert secret == "dummy"
    boto3_client("dynamodb").get_item.assert_called_with(
        TableName="dummyenv-dummystage-secrets",
        Key={"id": {"S": "sample_secret"}})

    # Call to the KMS client to decrypt the secret
    boto3_client('kms').decrypt.assert_called_with(CiphertextBlob="encrypted")


def test_get_secret_no_stage(boto3_resource, boto3_client, monkeypatch):
    """Gets a secret from DynamoDB without a deployment stage."""
    # Call to the DynamoDB client to retrieve the encrypted secret
    monkeypatch.setattr("boto3.resource", boto3_resource)
    monkeypatch.setattr("boto3.client", boto3_client)
    lambdautils.utils.get_secret("sample_secret", environment="dummyenv")
    boto3_client("dynamodb").get_item.assert_called_with(
        TableName="dummyenv-secrets",
        Key={"id": {"S": "sample_secret"}})

    # Call to the KMS client to decrypt the secret
    boto3_client('kms').decrypt.assert_called_with(CiphertextBlob="encrypted")


def test_get_secret_caller_scope(boto3_resource, boto3_client, monkeypatch):
    """Gets a secret from DynamoDB."""
    # Call to the DynamoDB client to retrieve the encrypted secret
    monkeypatch.setattr("boto3.resource", boto3_resource)
    monkeypatch.setattr("boto3.client", boto3_client)
    HUMILIS_ENVIRONMENT = "dummyenv"  # noqa
    HUMILIS_STAGE = "dummystage"      # noqa
    lambdautils.utils.get_secret("sample_secret")
    boto3_client("dynamodb").get_item.assert_called_with(
        TableName="dummyenv-dummystage-secrets",
        Key={"id": {"S": "sample_secret"}})

    # Call to the KMS client to decrypt the secret
    boto3_client('kms').decrypt.assert_called_with(CiphertextBlob="encrypted")


def test_get_state(boto3_resource, monkeypatch):
    """Gets a state value from DynamoDB."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    lambdautils.utils.get_state("sample_state_key", environment="dummyenv",
                                layer="dummylayer", stage="dummystage")
    boto3_resource("dynamodb").Table.assert_called_with(
        "dummyenv-dummylayer-dummystage-state")
    boto3_resource("dynamodb").Table().get_item.assert_called_with(
        Key={"id": "sample_state_key"})


def test_get_state_by_shard(boto3_resource, monkeypatch):
    """Gets a state value from DynamoDB."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    lambdautils.utils.get_state("sample_state_key", environment="dummyenv",
                                layer="dummylayer", stage="dummystage",
                                shard="shard-000001")
    boto3_resource("dynamodb").Table.assert_called_with(
        "dummyenv-dummylayer-dummystage-1-state")
    boto3_resource("dynamodb").Table().get_item.assert_called_with(
        Key={"id": "sample_state_key"})


def test_get_state_no_stage(boto3_resource, monkeypatch):
    """Gets a state value from DynamoDB without a deployment stage."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    lambdautils.utils.get_state("sample_state_key", environment="dummyenv",
                                layer="dummylayer")
    boto3_resource("dynamodb").Table().get_item.assert_called_with(
        Key={"id": "sample_state_key"})


def test_get_state_caller_scope(boto3_resource, monkeypatch):
    """Gets a state value from DynamoDB."""
    monkeypatch.setattr("boto3.resource", boto3_resource)

    def dummy_function():
        HUMILIS_ENVIRONMENT = "dummyenv"  # noqa
        HUMILIS_LAYER = "dummylayer"      # noqa
        HUMILIS_STAGE = "dummystage"      # noqa

        def dummy_function_2():
            return lambdautils.utils.get_state("sample_state_key")
        return dummy_function_2()

    dummy_function()

    boto3_resource("dynamodb").Table.assert_called_with(
        "dummyenv-dummylayer-dummystage-state")
    boto3_resource("dynamodb").Table().get_item.assert_called_with(
        Key={"id": "sample_state_key"})


def test_set_state_no_state_table(boto3_resource, monkeypatch):
    """Tests setting a state variable without having a state table."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    with pytest.raises(lambdautils.utils.StateTableError):
        lambdautils.utils.set_state("sample_state_key", "sample_state_value")


def test_set_state(boto3_resource, monkeypatch):
    """Tests setting a state variable."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    lambdautils.utils.set_state("sample_state_key", "sample_state_value",
                                environment="dummyenv", layer="dummylayer",
                                stage="dummystage")
    boto3_resource("dynamodb").Table().put_item.assert_called_with(
        Item={"id": "sample_state_key", "value": "sample_state_value"})


def test_sentry_monitor(boto3_client, raven_client, context, monkeypatch):
    """Tests the sentry_monitor decorator."""

    monkeypatch.setattr("raven.Client", Mock(return_value=raven_client))
    monkeypatch.setattr("boto3.client", boto3_client)

    @lambdautils.utils.sentry_monitor(environment="dummyenv",
                                      stage="dummystage")
    def lambda_handler(event, context):
        pass

    lambda_handler(None, context)
    raven_client.captureException.assert_not_called()
    boto3_client("dynamodb").get_item.assert_called_with(
        TableName="dummyenv-dummystage-secrets",
        Key={"id": {"S": "sentry.dsn"}})


def test_sentry_monitor_bad_client(boto3_client, raven_client, context,
                                   monkeypatch):
    """Tests that sentry_monitor handles raven client errors gracefully."""

    class ClientError(Exception):
        pass

    def raise_error(dsn):
        raise ClientError

    monkeypatch.setattr("raven.Client", Mock(side_effect=raise_error))
    monkeypatch.setattr("boto3.client", boto3_client)

    @lambdautils.utils.sentry_monitor(environment="dummyenv",
                                      stage="dummystage")
    def lambda_handler(event, context):
        pass

    lambda_handler(None, context)
    raven_client.captureException.assert_not_called()
    boto3_client("dynamodb").get_item.assert_called_with(
        TableName="dummyenv-dummystage-secrets",
        Key={"id": {"S": "sentry.dsn"}})


def test_sentry_monitor_exception_no_error_stream(
        boto3_client, raven_client, context, kinesis_event, monkeypatch):
    """Tests the sentry_monitor decorator when throwing an exception and
    lacking an error stream where to dump the errors."""
    monkeypatch.setattr("boto3.client", boto3_client)
    monkeypatch.setattr("raven.Client", Mock(return_value=raven_client))
    monkeypatch.setattr("lambdautils.utils.get_secret",
                        Mock(return_value="dummydsn"))

    # Needed to retrieve the sentry token
    HUMILIS_ENVIRONMENT = "dummyenv"   # noqa
    HUMILIS_STAGE = "dummystage"       # noqa

    @lambdautils.utils.sentry_monitor(environment="dummyenv",
                                      layer="dummylayer",
                                      stage="dummystage",
                                      error_stream="",
                                      error_delivery_stream="")
    def lambda_handler(event, context):
        raise KeyError

    with pytest.raises(lambdautils.utils.ErrorStreamError):
        lambda_handler(kinesis_event, context)

    # Should have captured 2 errors:
    # * The original KeyError
    # * The error raised when trying to deliver the error to a nonexisting
    #   error stream.
    assert raven_client.captureException.call_count == 2

    # And should have not send the events to the output stream
    boto3_client("kinesis").put_records.assert_not_called
    boto3_client("firehose").put_record_batch.assert_not_called


def test_sentry_monitor_exception_with_error_stream(
        boto3_client, raven_client, context, kinesis_event, monkeypatch):
    """Tests the sentry_monitor decorator when throwing an exception and
    lacking an error stream where to dump the errors."""
    monkeypatch.setattr("boto3.client", boto3_client)
    monkeypatch.setattr("raven.Client", Mock(return_value=raven_client))
    monkeypatch.setattr("lambdautils.utils.get_secret",
                        Mock(return_value="dummydsn"))

    # Needed to retrieve the sentry token
    HUMILIS_ENVIRONMENT = "dummyenv"   # noqa
    HUMILIS_STAGE = "dummystage"       # noqa

    @lambdautils.utils.sentry_monitor(environment="dummyenv",
                                      layer="dummylayer",
                                      stage="dummystage",
                                      error_stream="ErrorStream",
                                      error_delivery_stream="ErrorStream")
    def lambda_handler(event, context):
        raise KeyError

    # Should not raise and just send the events to the error stream
    lambda_handler(kinesis_event, context)

    # Should have captured only 1 error:
    # * The original KeyError
    assert raven_client.captureException.call_count == 1

    # And should have send the events to the Kinesis and FH error streams
    assert boto3_client("kinesis").put_records.call_count == 1
    assert boto3_client("firehose").put_record_batch.call_count == 1


def test_context_dict(context):
    """Tests utility context_dict."""
    d = lambdautils.utils.context_dict(context)
    assert len(d) == 8
    assert d["function_name"] == context.function_name


def test_send_to_kinesis_stream(search_events, boto3_client, monkeypatch):
    """Tests sending events to a Kinesis stream."""
    monkeypatch.setattr("boto3.client", boto3_client)
    lambdautils.utils.send_to_kinesis_stream(search_events, "dummy_stream")
    boto3_client("kinesis").put_records.call_count == 1


def test_send_to_delivery_stream(search_events, boto3_client, monkeypatch):
    """Tests sending events to a Firehose delivery stream."""
    monkeypatch.setattr("boto3.client", boto3_client)
    lambdautils.utils.send_to_delivery_stream(search_events, "dummy_stream")
    boto3_client("firehose").put_record_batch.call_count == 1


def test_unpack_kinesis_event(kinesis_event):
    """Extracts json-serialized events from a Kinesis events."""
    events = lambdautils.utils.unpack_kinesis_event(kinesis_event,
                                                    deserializer=json.loads)
    # There should be one event per kinesis record
    assert len(events) == len(kinesis_event["Records"])


def test_send_cf_response(cf_kinesis_event, cf_context, monkeypatch):
    """Tests sending a response to Cloudformation."""
    monkeypatch.setattr("urllib2.build_opener", Mock())
    mocked_request = Mock()
    monkeypatch.setattr("urllib2.Request", mocked_request)
    lambdautils.utils.send_cf_response(cf_kinesis_event, cf_context, "SUCCESS",
                                       reason="reason", response_data="data",
                                       physical_resource_id="id")
    response_body = json.dumps(
        {
            'Status': "SUCCESS",
            'Reason': "reason",
            'PhysicalResourceId': "id",
            'StackId': cf_kinesis_event['StackId'],
            'RequestId': cf_kinesis_event['RequestId'],
            'LogicalResourceId': cf_kinesis_event['LogicalResourceId'],
            'Data': "data"
        }
    )
    mocked_request.assert_called_with(cf_kinesis_event["ResponseURL"],
                                      data=response_body)
