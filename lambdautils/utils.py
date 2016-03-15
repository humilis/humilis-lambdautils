# -*- coding: utf-8 -*-
"""Utilities for Lambda functions deployed using humilis."""

import base64
import inspect
import json
import logging
import os
import traceback
import uuid

import boto3
from botocore.exceptions import ClientError
import raven


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class StateTableError(Exception):
    pass


class ErrorStreamError(Exception):
    pass


class RequiresStreamNameError(Exception):
    pass


def _error_stream_name(environment=None, stage=None):
    """The name of the Kinesis stream used to capture errors."""
    if environment is None:
        # For backwards compatiblity
        environment = os.environ.get("HUMILIS_ENVIRONMENT") or \
            _calling_scope_variable("HUMILIS_ENVIRONMENT")

    if stage is None:
        stage = os.environ.get("HUMILIS_STAGE") or \
            _calling_scope_variable("HUMILIS_STAGE")

    if environment:
        if stage:
            return "{environment}{stage}Error".format(
                environment=environment.title(), stage=stage.title())
        else:
            return "{environment}Error".format(environment=environment.title())


def _secrets_table_name(environment=None, stage=None):
    """The name of the secrets table associated to a humilis deployment."""
    if environment is None:
        # For backwards compatiblity
        environment = os.environ.get("HUMILIS_ENVIRONMENT") or \
            _calling_scope_variable("HUMILIS_ENVIRONMENT")

    if stage is None:
        stage = os.environ.get("HUMILIS_STAGE") or \
            _calling_scope_variable("HUMILIS_STAGE")

    if environment:
        if stage:
            return "{environment}-{stage}-secrets".format(**locals())
        else:
            return "{environment}-secrets".format(**locals())


def _state_table_name(environment=None, layer=None, stage=None):
    """The name of the state table associated to a humilis deployment."""
    if environment is None:
        # For backwards compatiblity
        environment = os.environ.get("HUMILIS_ENVIRONMENT") or \
            _calling_scope_variable("HUMILIS_ENVIRONMENT")
    if layer is None:
        layer = os.environ.get("HUMILIS_LAYER") or \
            _calling_scope_variable("HUMILIS_LAYER")

    if stage is None:
        stage = os.environ.get("HUMILIS_STAGE") or \
            _calling_scope_variable("HUMILIS_STAGE")

    if environment:
        if stage:
            return "{environment}-{layer}-{stage}-state".format(**locals())
        else:
            return "{environment}-{layer}-state".format(**locals())


def _calling_scope_variable(name):
    """Looks for a variable in the calling scopes."""
    frame = inspect.stack()[1][0]
    while name not in frame.f_locals:
        frame = frame.f_back
        if frame is None:
            return None
    return frame.f_locals[name]


def get_secret(key, environment=None, stage=None):
    """Retrieves a secret from the secrets vault."""
    # Get the encrypted secret from DynamoDB
    table_name = _secrets_table_name(environment=environment, stage=stage)
    if table_name is None:
        logger.warning("Can't produce secrets table name: unable to retrieve "
                       "secret '{}'".format(key))
        return

    client = boto3.client('dynamodb')
    try:
        encrypted = client.get_item(
            TableName=table_name,
            Key={'id': {'S': key}}).get('Item', {}).get('value', {}).get('B')
    except ClientError:
        logger.info("DynamoDB error when retrieving secret '{}'".format(key))
        traceback.print_exc()
        return

    if encrypted is None:
        return

    # Decrypt using KMS
    client = boto3.client('kms')
    try:
        value = client.decrypt(CiphertextBlob=encrypted)['Plaintext'].decode()
    except ClientError:
        logger.error("KMS error when trying to decrypt secret")
        traceback.print_exc()
        return

    try:
        value = json.loads(value)
    except (TypeError, ValueError):
        # It's ok, the client should know how to deal with the value
        pass

    return value


def get_state(key, table_name=None, environment=None, layer=None, stage=None):
    """Gets a state value from the state table."""
    if table_name is None:
        table_name = _state_table_name(environment=environment, layer=layer,
                                       stage=stage)

    if not table_name:
        logger.warning("Can't produce state table name: unable to retrieve "
                       "state item '{}'".format(key))
        return

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    logger.info("Getting key '{}' from table '{}'".format(key, table_name))
    try:
        value = table.get_item(Key={"id": key}).get("Item", {}).get("value")
    except ClientError:
        logger.warning("DynamoDB error when retrieving key '{}' from table "
                       "'{}'".format(key, table_name))
        traceback.print_exc()
        return

    try:
        value = json.loads(value)
    except (TypeError, ValueError):
        # It's ok, the client should know how to deal with the value
        pass

    return value


def set_state(key, value, table_name=None, environment=None, layer=None,
              stage=None):
    """Sets a state value."""
    if table_name is None:
        table_name = _state_table_name(environment=environment, layer=layer,
                                       stage=stage)

    if not table_name:
        msg = ("Can't produce state table name: unable to set state "
               "item '{}'".format(key))
        logger.error(msg)
        raise StateTableError(msg)
        return
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    logger.info("Putting {} -> {} in DynamoDB table {}".format(key, value,
                                                               table_name))
    if not isinstance(value, str):
        # Serialize using json
        try:
            value = json.dumps(value)
        except TypeError:
            logger.warning("Unable to json-serialize state '{}".format(
                key))
            # Try to store the value as it is

    resp = table.put_item(Item={"id": key, "value": value})
    logger.info("Response from DynamoDB: '{}'".format(resp))
    return resp


def send_to_delivery_stream(events, stream_name):
    """Sends a list of events to a Firehose delivery stream."""
    records = []
    if stream_name is None:
        msg = "Must provide the name of the Kinesis stream: None provided"
        logger.error(msg)
        raise RequiresStreamNameError(msg)
    for event in events:
        if not isinstance(event, str):
            # csv events already have a newline
            event = json.dumps(event) + "\n"
        records.append({"Data": event})
    firehose = boto3.client("firehose")
    logger.info("Delivering {} records to Firehose stream '{}'".format(
        len(records), stream_name))
    resp = firehose.put_record_batch(
        DeliveryStreamName=stream_name,
        Records=records)
    return resp


def send_to_kinesis_stream(events, stream_name, partition_key=None):
    """Sends events to a Kinesis stream."""
    records = []
    if stream_name is None:
        msg = "Must provide the name of the Kinesis stream: None provided"
        logger.error(msg)
        raise RequiresStreamNameError(msg)
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


def sentry_monitor(environment=None, stage=None, layer=None):
    def decorator(func):
        """A decorator that adds Sentry monitoring to a Lambda handler."""
        def wrapper(event, context):
            logger.info("Retrieving Sentry DSN for environment '{}' and "
                        "stage '{}'".format(environment, stage))
            dsn = get_secret("sentry.dsn",
                             environment=environment,
                             stage=stage)

            if dsn is None:
                logger.warning("Unable to retrieve sentry DSN")
            else:
                client = raven.Client(dsn)
            if dsn is not None:
                client.user_context(context_dict(context))
                try:
                    return func(event, context)
                except:
                    client.captureException()
                    # Send the failed payloads to the errored events to the
                    # error stream and resume
                    error_stream = None
                    try:
                        payloads = unpack_kinesis_event(event,
                                                        deserializer=None)

                        # Add info about the error so that we are able to
                        # repush the events to the right place after fixing
                        # them.
                        error_payloads = [
                            {"environment": environment,
                             "layer": layer,
                             "stage": stage,
                             "payload": payloads} for payload in payloads]

                        error_stream = _error_stream_name(
                            environment=environment, stage=stage)

                        # Note: we assume that both the Kinesis and Firehose
                        # streams have the same name.
                        send_to_kinesis_stream(error_payloads, error_stream)
                        send_to_delivery_stream(error_payloads, error_stream)
                    except:
                        client.captureException()
                        try:
                            msg = ("Error delivering errors to Error "
                                   "stream '{}'".format(error_stream))
                            logger.error(msg)
                            raise ErrorStreamError(msg)
                        except:
                            client.captureException()
                            # In this case we do need to raise of we will loose
                            # data
                            raise
                    # If we were able to deliver the error events to the error
                    # stream, we let it pass to prevent blocking the whole
                    # pipeline.
                    pass
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


def unpack_kinesis_event(kinesis_event, deserializer=None):
    """Extracts events (a list of dicts) from a Kinesis event."""
    records = kinesis_event["Records"]
    events = []
    for rec in records:
        payload = base64.decodestring(rec["kinesis"]["data"]).decode()
        if deserializer:
            try:
                payload = json.loads(payload)
            except ValueError:
                logger.error("Error deserializing Kinesis payload: {}".format(
                    payload))
                raise
        events.append(payload)

    return events
