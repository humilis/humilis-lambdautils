"""Unit tests."""

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

import lambdautils.utils


@pytest.mark.parametrize(
    "key,environment,stage,namespace,table,nkey", [
        ("k", "e", "s", None, "e-s-secrets", "k"),
        ("k", "e", None, None, "e-dummystage-secrets", "k"),
        ("k", "e", None, "n", "e-dummystage-secrets", "n:k"),
        ("k", "e", "s", "n", "e-s-secrets", "n:k")])
def test_get_secret(key, environment, stage, namespace, table, nkey,
                    boto3_resource, boto3_client, monkeypatch):
    """Gets a secret from DynamoDB."""
    # Call to the DynamoDB client to retrieve the encrypted secret
    monkeypatch.setattr("boto3.resource", boto3_resource)
    monkeypatch.setattr("boto3.client", boto3_client)
    secret = lambdautils.utils.get_secret(key,
                                          namespace=namespace,
                                          environment=environment,
                                          stage=stage)
    assert secret == "dummy"
    boto3_client("dynamodb").get_item.assert_called_with(
        TableName=table,
        Key={"id": {"S": nkey}})

    # Call to the KMS client to decrypt the secret
    boto3_client('kms').decrypt.assert_called_with(CiphertextBlob="encrypted")


def test_get_environment_setting(monkeypatch):
    """Should be an alias for get_secret."""
    resp = str(uuid.uuid4())
    arg = str(uuid.uuid4())
    kwarg = str(uuid.uuid4())
    get_secret = Mock(return_value=resp)
    monkeypatch.setattr("lambdautils.state.get_secret", get_secret)
    resp2 = lambdautils.state.get_environment_setting(arg, kwarg=kwarg)
    assert resp2 == resp
    get_secret.assert_called_with(arg, kwarg=kwarg)


@pytest.mark.parametrize(
    "key,environment,layer,stage,shard_id,namespace,table,consistent,nkey", [
        ("k", "e", "l", "s", None, None, "e-l-s-state", False, "k"),
        ("k", "e", "l", "s", None, "n", "e-l-s-state", False, "n:k"),
        ("k", "e", "l", "s", "s-012", "n", "e-l-s-state", True, "s-012:n:k"),
        ("k", "e", "l", "s", "s-0001", None, "e-l-s-state", True, "s-0001:k")])
def test_get_state(boto3_resource, monkeypatch, key, environment, layer,
                   stage, shard_id, namespace, table, consistent, nkey):
    """Get a state value from DynamoDB."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    lambdautils.utils.get_state(key, environment=environment, layer=layer,
                                stage=stage, shard_id=shard_id,
                                namespace=namespace,
                                consistent=consistent)
    boto3_resource("dynamodb").Table.assert_called_with(table)
    if consistent is None:
        # The default setting: use consistent reads
        consistent = True
    boto3_resource("dynamodb").Table().get_item.assert_called_with(
        Key={"id": nkey}, ConsistentRead=consistent)


def test_no_state_table(boto3_resource, monkeypatch):
    """Test accessing state variable without having a state table."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    monkeypatch.delenv("HUMILIS_ENVIRONMENT")
    with pytest.raises(lambdautils.state.StateTableError):
        lambdautils.utils.set_state("sample_state_key", "sample_state_value")

    with pytest.raises(lambdautils.state.StateTableError):
        lambdautils.utils.delete_state("sample_state_key")

    with pytest.raises(lambdautils.state.StateTableError):
        lambdautils.utils.get_state("sample_state_key")


@pytest.mark.parametrize(
    "key,value,environment,layer,stage,shard_id,namespace,table,nkey", [
        ("k", "v", "e", "l", "s", None, None, "e-l-s-state", "k"),
        ("k", "v", "e", "l", "s", None, "n", "e-l-s-state", "n:k"),
        ("k", "v", "e", "l", "s", "s1", "n", "e-l-s-state", "s1:n:k"),
        ("k", "v", "e", "l", "s", "s2", None, "e-l-s-state", "s2:k")])
def test_set_state(boto3_resource, monkeypatch, key, value, environment, layer,
                   stage, shard_id, namespace, table, nkey):
    """Tests setting a state variable."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    lambdautils.utils.set_state(key, value, environment=environment,
                                layer=layer, stage=stage, shard_id=shard_id,
                                namespace=namespace)
    boto3_resource("dynamodb").Table.assert_called_with(table)
    boto3_resource("dynamodb").Table().put_item.assert_called_with(
        Item={"id": nkey, "value": json.dumps(value)})


@pytest.mark.parametrize(
    "key,environment,layer,stage,shard_id,namespace,table,nkey", [
        ("k", "e", "l", "s", None, None, "e-l-s-state", "k"),
        ("k", "e", "l", "s", None, "n", "e-l-s-state", "n:k"),
        ("k", "e", "l", "s", "s1", "n", "e-l-s-state", "s1:n:k"),
        ("k", "e", "l", "s", "s2", None, "e-l-s-state", "s2:k")])
def test_delete_state(boto3_resource, monkeypatch, key, environment,
                      layer, stage, shard_id, namespace, table, nkey):
    """Tests setting a state variable."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    lambdautils.utils.delete_state(key, environment=environment,
                                   layer=layer, stage=stage, shard_id=shard_id,
                                   namespace=namespace)
    boto3_resource("dynamodb").Table.assert_called_with(table)
    boto3_resource("dynamodb").Table().delete_item.assert_called_with(
        Key={"id": nkey})


def test_sentry_monitor_bad_client(boto3_client, raven_client, context,
                                   monkeypatch):
    """Test that sentry_monitor handles raven client errors gracefully."""

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


@pytest.mark.parametrize(
    "kstream, fstream, rcalls, kcalls, fcalls, ev", [
        ("a", "b", 0, 1, 1, {"Records": [{}]}),
        (None, "b", 0, 0, 1, {"Records": [{}]}),
        (None, None, 1, 0, 0, None),
        (None, None, 1, 0, 0, None),
        ("a", "b", 0, 1, 1, None),
        ("a", None, 0, 1, 0, None)])
def test_sentry_monitor_exception(
        kstream, fstream, rcalls, kcalls, fcalls, ev,
        boto3_client, raven_client, context, kinesis_event, monkeypatch):
    """Tests the sentry_monitor decorator when throwing an exception and
    lacking an error stream where to dump the errors."""

    if ev is None:
        # Default to a Kinesis event
        ev = kinesis_event

    monkeypatch.setattr("boto3.client", boto3_client)
    monkeypatch.setattr("raven.Client", Mock(return_value=raven_client))
    monkeypatch.setattr("lambdautils.monitor.logger", Mock())
    monkeypatch.setattr("lambdautils.monitor.SentryHandler", Mock())
    monkeypatch.setattr("lambdautils.utils.get_secret",
                        Mock(return_value="dummydsn"))

    error_stream = {
        "kinesis_stream": kstream,
        "firehose_delivery_stream": fstream}

    @lambdautils.utils.sentry_monitor(error_stream=error_stream)
    def lambda_handler(event, context):
        """Raise an error."""
        raise KeyError

    if not kstream and not fstream:
        with pytest.raises(KeyError):
            lambda_handler(ev, context)
    else:
        lambda_handler(ev, context)

    # Should have captured only 1 error:
    # * The original KeyError
    assert raven_client.captureException.call_count == rcalls

    # And should have send the events to the Kinesis and FH error streams
    assert boto3_client("kinesis").put_records.call_count == kcalls
    assert boto3_client("firehose").put_record_batch.call_count == fcalls


def test_sentry_monitor_critical_exception(context, kinesis_event,
                                           boto3_client, raven_client,
                                           monkeypatch):
    """Tests that sentry_monitor reraises critical exceptions."""

    monkeypatch.setattr("boto3.client", boto3_client)
    monkeypatch.setattr("raven.Client", Mock(return_value=raven_client))
    monkeypatch.setattr("logging.getLogger", Mock())
    monkeypatch.setattr("logging.NullHandler", Mock())
    monkeypatch.setattr("lambdautils.monitor.SentryHandler", Mock())

    @lambdautils.utils.sentry_monitor(environment="dummyenv",
                                      layer="dummylayer",
                                      stage="dummystage")
    def lambda_handler(event, context):
        raise lambdautils.utils.CriticalError(KeyError)

    with pytest.raises(lambdautils.utils.CriticalError):
        lambda_handler(kinesis_event, context)


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


@pytest.mark.parametrize("deserializer, embed_ts", [
    [json.loads, False],
    [json.loads, "kinesis_timestamp"],
    [None, False]])
def test_unpack_kinesis_event(kinesis_event, deserializer, embed_ts):
    """Extracts json-serialized events from a Kinesis events."""
    events, shard_id = lambdautils.utils.unpack_kinesis_event(
        kinesis_event, deserializer=deserializer, embed_timestamp=embed_ts)
    # There should be one event per kinesis record
    assert len(events) == len(kinesis_event["Records"])
    assert shard_id == kinesis_event["Records"][0]["eventID"].split(":")[0]
    if embed_ts:
        assert all(embed_ts in ev for ev in events)
