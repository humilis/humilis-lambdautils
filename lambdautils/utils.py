"""Utilities for Lambda functions deployed using humilis."""

import base64
from datetime import datetime
from dateutil import tz
import json
import logging
import os
import traceback
import urllib2
import uuid

import boto3
from botocore.exceptions import ClientError
import raven


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class CriticalError(Exception):
    def __init__(self, exception):
        self.__exception = exception

    def __str__(self):
        return str(self.__exception)


class StateTableError(Exception):
    pass


class ErrorStreamError(Exception):
    pass


class RequiresStreamNameError(Exception):
    pass


class BadKinesisEventError(Exception):
    pass


def _secrets_table_name(environment=None, stage=None):
    """The name of the secrets table associated to a humilis deployment."""
    if environment is None:
        environment = os.environ.get("HUMILIS_ENVIRONMENT")

    if stage is None:
        stage = os.environ.get("HUMILIS_STAGE")

    if environment:
        if stage:
            return "{environment}-{stage}-secrets".format(**locals())
        else:
            return "{environment}-secrets".format(**locals())


def _state_table_name(environment=None, layer=None, stage=None):
    """The name of the state table associated to a humilis deployment."""
    if environment is None:
        # For backwards compatiblity
        environment = os.environ.get("HUMILIS_ENVIRONMENT")
    if layer is None:
        layer = os.environ.get("HUMILIS_LAYER")

    if stage is None:
        stage = os.environ.get("HUMILIS_STAGE")

    if environment:
        if stage:
            return "{environment}-{layer}-{stage}-state".format(
                **locals())
        else:
            return "{environment}-{layer}-state".format(**locals())


def get_secret(key, environment=None, stage=None, namespace=None):
    """Retrieves a secret from the secrets vault."""
    # Get the encrypted secret from DynamoDB
    table_name = _secrets_table_name(environment=environment, stage=stage)

    if namespace:
        key = "{}:{}".format(namespace, key)

    if table_name is None:
        logger.warning("Can't produce secrets table name: unable to retrieve "
                       "secret '{}'".format(key))
        return

    client = boto3.client('dynamodb')
    try:
        logger.info("Retriving key '{}' from table '{}'".format(
            key, table_name))
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


def get_state(key, namespace=None, table_name=None, environment=None,
              layer=None, stage=None, shard_id=None,
              consistent=None, deserializer=json.loads):
    """Gets a state value from the state table."""
    if consistent is None:
        consistent = True
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
        if namespace:
            key = "{}:{}".format(namespace, key)

        if shard_id:
            key = "{}:{}".format(shard_id, key)

        value = table.get_item(
            Key={"id": key}, ConsistentRead=consistent).get(
                "Item", {}).get("value")
    except ClientError:
        logger.warning("DynamoDB error when retrieving key '{}' from table "
                       "'{}'".format(key, table_name))
        traceback.print_exc()
        return

    try:
        if deserializer:
            value = deserializer(value)
    except (TypeError, ValueError):
        # It's ok, the client should know how to deal with the value
        pass

    return value


def set_state(key, value, table_name=None, environment=None, layer=None,
              stage=None, shard_id=None, namespace=None,
              serializer=json.dumps):
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
            value = serializer(value)
        except TypeError:
            logger.warning("Unable to json-serialize state '{}".format(
                key))
            # Try to store the value as it is
    elif serializer:
        logger.warning("Value is already a string: not serializing")

    if namespace:
        key = "{}:{}".format(namespace, key)

    if shard_id:
        key = "{}:{}".format(shard_id, key)

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


def send_to_kinesis_stream(events, stream_name, partition_key=None,
                           serializer=json.dumps):
    """Sends events to a Kinesis stream."""
    records = []
    if stream_name is None:
        msg = "Must provide the name of the Kinesis stream: None provided"
        logger.error(msg)
        raise RequiresStreamNameError(msg)
    for event in events:
        if partition_key is None:
            partition_key_value = str(uuid.uuid4())
        elif hasattr(partition_key, "__call__"):
            partition_key_value = partition_key(event)
        else:
            partition_key_value = partition_key

        if not isinstance(event, str):
            event = serializer(event)

        record = {"Data": event,
                  "PartitionKey": partition_key_value}
        records.append(record)

    kinesis = boto3.client("kinesis")
    resp = kinesis.put_records(StreamName=stream_name, Records=records)
    return resp


def sentry_monitor(environment=None, stage=None, layer=None,
                   error_stream=None):
    if not error_stream:
        error_stream = {}
    config = {
        "environment": environment,
        "stage": stage,
        "layer": layer,
        "mapper": error_stream.get("mapper"),
        "filter": error_stream.get("filter"),
        "partition_key": error_stream.get("partition_key"),
        "error_stream": error_stream.get("kinesis_stream"),
        "error_delivery_stream": error_stream.get("firehose_delivery_stream")}
    logger.info("Environment config: {}".format(config))

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
                try:
                    client = raven.Client(dsn)
                except:
                    # We don't want to break the application. Add some retry
                    # logic later.
                    logger.error("Raven client error: skipping Sentry")
                    logger.error(traceback.print_exc())
                    dsn = None

            if dsn is not None:
                client.user_context(context_dict(context))

            try:
                return func(event, context)
            except CriticalError as err:
                logger.error(err)
                if dsn is not None:
                    client.captureException()
                raise
            except:
                if dsn is not None:
                    try:
                        client.captureException()
                    except:
                        logger.error("Raven error capturing exception")
                        logger.error(traceback.print_exc())
                        raise

                try:
                    # Send the failed payloads to the errored events to the
                    # error stream and resume
                    if not config["error_stream"] \
                            and not config["error_delivery_stream"]:
                        msg = ("Error sending errors to Error stream: "
                               "no error streams were provided")
                        logger.error(msg)
                        raise
                    try:
                        # Try unpacking as if it were a Kinesis event
                        payloads, shard_id = unpack_kinesis_event(
                            event, deserializer=None)
                    except KeyError:
                        # If not a Kinesis event, just unpack the records
                        payloads = event["Records"]
                        shard_id = None

                    # Add info about the error so that we are able to
                    # repush the events to the right place after fixing
                    # them.
                    error_payloads = []
                    fc = config["filter"]
                    mc = config["mapper"]
                    state_args = dict(environment=environment, layer=layer,
                                      stage=stage, shard_id=shard_id)
                    for p in payloads:
                        if fc and not fc(p, state_args):
                            continue
                        if mc:
                            mc(p, state_args)

                        payload = {
                            "context": state_args,
                            "payload": p}

                        error_payloads.append(payload)

                    logger.info("Error payloads: {}".format(
                        json.dumps(error_payloads, indent=4)))

                    if not error_payloads:
                        logger.info("All error payloads were filtered out: "
                                    "will silently ignore the errors")
                        return

                    if config["error_stream"]:
                        send_to_kinesis_stream(
                            error_payloads, config["error_stream"],
                            partition_key=config["partition_key"])
                        logger.info("Sent payload to Kinesis stream "
                                    "'{}'".format(error_stream))
                    else:
                        logger.info("No error stream specified: skipping")

                    if config["error_delivery_stream"]:
                        send_to_delivery_stream(
                            error_payloads, config["error_delivery_stream"])
                        msg = ("Sent payload to Firehose delivery stream "
                               "'{}'").format(config["error_delivery_stream"])
                        logger.info(msg)
                    else:
                        logger.info("No delivery stream specified: skipping")

                except:
                    if dsn is not None:
                        try:
                            client.captureException()
                        except:
                            logger.error("Raven error capturing exception")
                            logger.error(traceback.print_exc())

                    msg = "Error delivering errors to Error stream(s)"
                    logger.error(msg)
                    raise
                # If we were able to deliver the error events to the error
                # stream, we silent the exception to prevent blocking the
                # pipeline.
                pass
        return wrapper
    return decorator


def in_aws_lambda():
    """Returns true if running in AWS Lambda service."""
    return "AWS_SESSION_TOKEN" in os.environ \
        and "AWS_SESSION_TOKEN" in os.environ


def context_dict(context):
    """Creates a dict with context information for Sentry."""
    d = {
        "function_name": context.function_name,
        "function_version": context.function_version,
        "invoked_function_arn": context.invoked_function_arn,
        "memory_limit_in_mb": context.memory_limit_in_mb,
        "aws_request_id": context.aws_request_id,
        "log_group_name": context.log_group_name,
        "cognito_identity_id": context.identity.cognito_identity_id,
        "cognito_identity_pool_id": context.identity.cognito_identity_pool_id}
    for k, v in os.environ.items():
        if k not in {"AWS_SECURITY_TOKEN", "AWS_SESSION_TOKEN",
                     "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}:
            # Do not log credentials
            d[k] = v

    return d


def unpack_kinesis_event(kinesis_event, deserializer=None,
                         embed_timestamp=False):
    """Extracts events (a list of dicts) from a Kinesis event."""
    records = kinesis_event["Records"]
    events = []
    shard_ids = set()
    for rec in records:
        payload = base64.decodestring(rec["kinesis"]["data"]).decode()
        shard_ids.add(rec["eventID"].split(":")[0])
        if deserializer:
            try:
                payload = deserializer(payload)
            except ValueError:
                logger.error("Error deserializing Kinesis payload: {}".format(
                    payload))
                raise

        if isinstance(payload, dict) and embed_timestamp:
            ts = rec["kinesis"].get("approximateArrivalTimestamp")
            if ts:
                ts = datetime.fromtimestamp(ts, tz=tz.tzutc())
                ts_str = ("{year:04d}-{month:02d}-{day:02d} "
                          "{hour:02d}:{minute:02d}:{second:02d}").format(
                    year=ts.year,
                    month=ts.month,
                    day=ts.day,
                    hour=ts.hour,
                    minute=ts.minute,
                    second=ts.second)
            else:
                ts_str = ""

            payload[embed_timestamp] = ts_str
        events.append(payload)

    if len(shard_ids) > 1:
        msg = "Kinesis event contains records from several shards: {}".format(
            shard_ids)
        raise(BadKinesisEventError(msg))

    return events, shard_ids.pop()


def send_cf_response(event, context, response_status, reason=None,
                     response_data=None, physical_resource_id=None):
    """Responds to Cloudformation after a create/update/delete operation."""
    response_data = response_data or {}
    reason = reason or "See the details in CloudWatch Log Stream: " + \
        context.log_stream_name
    physical_resource_id = physical_resource_id or context.log_stream_name
    response_body = json.dumps(
        {
            'Status': response_status,
            'Reason': reason,
            'PhysicalResourceId': physical_resource_id,
            'StackId': event['StackId'],
            'RequestId': event['RequestId'],
            'LogicalResourceId': event['LogicalResourceId'],
            'Data': response_data
        }
    )

    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(event["ResponseURL"], data=response_body)
    request.add_header("Content-Type", "")
    request.add_header("Content-Length", len(response_body))
    request.get_method = lambda: 'PUT'
    try:
        response = opener.open(request)
        print("Status code: {}".format(response.getcode()))
        print("Status message: {}".format(response.msg))
        return True
    except urllib2.HTTPError as exc:
        print("Failed executing HTTP request: {}".format(exc.code))
        return False
