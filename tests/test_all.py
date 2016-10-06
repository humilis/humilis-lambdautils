"""Unit tests."""

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


@pytest.mark.parametrize(
    "key,environment,stage,namespace,table,nkey", [
        ("k", "e", "s", None, "e-s-secrets", "k"),
        ("k", "e", None, None, "e-secrets", "k"),
        ("k", "e", None, "n", "e-secrets", "n:k"),
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


@pytest.mark.parametrize(
    "key,environment,layer,stage,shard_id,namespace,table,consistent,nkey", [
        ("k", "e", "l", "s", None, None, "e-l-s-state", False, "k"),
        ("k", "e", "l", "s", None, "n", "e-l-s-state", False, "n:k"),
        ("k", "e", "l", "s", "s-012", "n", "e-l-s-state", True, "s-012:n:k"),
        ("k", "e", "l", None, "s-012", "n", "e-l-state", True, "s-012:n:k"),
        ("k", "e", "l", "s", "s-0001", None, "e-l-s-state", True, "s-0001:k")])
def test_get_state(boto3_resource, monkeypatch, key, environment, layer,
                   stage, shard_id, namespace, table, consistent, nkey):
    """Gets a state value from DynamoDB."""
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


def test_set_state_no_state_table(boto3_resource, monkeypatch):
    """Tests setting a state variable without having a state table."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    with pytest.raises(lambdautils.utils.StateTableError):
        lambdautils.utils.set_state("sample_state_key", "sample_state_value")


@pytest.mark.parametrize(
    "key,value,environment,layer,stage,shard_id,namespace,table,nkey", [
        ("k", "v", "e", "l", "s", None, None, "e-l-s-state", "k"),
        ("k", "v", "e", "l", "s", None, "n", "e-l-s-state", "n:k"),
        ("k", "v", "e", "l", "s", "s1", "n", "e-l-s-state", "s1:n:k"),
        ("k", "v", "e", "l", None, "s-00012", "n", "e-l-state", "s-00012:n:k"),
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
        ("k", "e", "l", None, "s-00012", "n", "e-l-state", "s-00012:n:k"),
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


def test_graphite_monitor(context, boto3_client, monkeypatch):
    sock = Mock()
    sock.sendto = Mock()
    monkeypatch.setattr("lambdautils.monitor.sock", sock)
    monkeypatch.setattr("lambdautils.monitor.GRAPHITE_HOST", "H")
    monkeypatch.setattr("lambdautils.monitor.GRAPHITE_PORT", "P")
    monkeypatch.setattr("boto3.client", boto3_client)

    @lambdautils.monitor.graphite_monitor(
        "metric", environment="env", stage="stage", layer="layer")
    def func():
        return True

    val = func()
    assert val is True
    sock.sendto.assert_called_with("dummy.env.layer.stage.metric:1|c",
                                   ("H", "P"))


def test_graphite_monitor_envars(context, boto3_client, monkeypatch):
    sock = Mock()
    sock.sendto = Mock()
    monkeypatch.setattr("lambdautils.monitor.sock", sock)
    monkeypatch.setattr("lambdautils.monitor.GRAPHITE_HOST", "H")
    monkeypatch.setattr("lambdautils.monitor.GRAPHITE_PORT", "P")
    monkeypatch.setattr("boto3.client", boto3_client)
    monkeypatch.setenv("HUMILIS_ENVIRONMENT", "env")
    monkeypatch.setenv("HUMILIS_LAYER", "layer")
    monkeypatch.setenv("HUMILIS_STAGE", "stage")

    @lambdautils.monitor.graphite_monitor("metric")
    def func():
        return True

    val = func()
    assert val is True
    sock.sendto.assert_called_with("dummy.env.layer.stage.metric:1|c",
                                   ("H", "P"))


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


@pytest.mark.parametrize(
    "kstream, fstream, mapper, filter, rcalls, kcalls, fcalls, ev", [
        ("a", "b", None, None, 0, 1, 1, {"Records": [{}]}),
        (None, "b", None, None, 0, 0, 1, {"Records": [{}]}),
        (None, None, None, None, 1, 0, 0, None),
        (None, None, lambda x, sa: x, lambda x, sa: True, 1, 0, 0, None),
        ("a", "b", None, lambda x, sa: True, 0, 1, 1, None),
        ("a", "b", None, lambda x, sa: False, 0, 0, 0, None),
        ("a", "b", lambda x, sa: x, lambda x, sa: False, 0, 0, 0, None),
        ("a", "b", lambda x, sa: x, lambda x, sa: True, 0, 1, 1, None),
        ("a", None, None, None, 0, 1, 0, None)])
def test_sentry_monitor_exception_with_error_stream(
        kstream, fstream, mapper, filter, rcalls, kcalls, fcalls, ev,
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
        "firehose_delivery_stream": fstream,
        "mapper": mapper,
        "filter": filter}

    @lambdautils.utils.sentry_monitor(environment="dummyenv",
                                      layer="dummylayer",
                                      stage="dummystage",
                                      error_stream=error_stream)
    def lambda_handler(event, context):
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


def test_send_cf_response(cf_kinesis_event, cf_context, monkeypatch):
    """Tests sending a response to Cloudformation."""
    monkeypatch.setattr("lambdautils.utils.build_opener", Mock())
    mocked_request = Mock()
    monkeypatch.setattr("lambdautils.utils.Request", mocked_request)
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


def test_in_aws_lambda(monkeypatch):
    """Tests in_the_cloud."""
    assert not lambdautils.utils.in_aws_lambda()
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "token")
    assert lambdautils.utils.in_aws_lambda()


@pytest.mark.parametrize("exception", [
    lambdautils.utils.CriticalError,
    lambdautils.utils.StateTableError,
    lambdautils.utils.BadKinesisEventError])
def test_exceptions(exception):
    """Tests the exceptions defined by lambdautils."""
    with pytest.raises(exception):
        raise exception("Nasty error")
