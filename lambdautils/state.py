"""Utilities to manage Lambda and environment state."""

import json
import logging
import os
import traceback

import boto3
from botocore.exceptions import ClientError
from retrying import retry

from .exception import CriticalError, StateTableError

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _secrets_table_name(environment=None, stage=None):
    """Name of the secrets table associated to a humilis deployment."""
    if environment is None:
        environment = os.environ.get("HUMILIS_ENVIRONMENT")

    if stage is None:
        stage = os.environ.get("HUMILIS_STAGE")

    if environment:
        if stage:
            return "{environment}-{stage}-secrets".format(**locals())
        else:
            return "{environment}-secrets".format(**locals())


def _environment_settings_table_name(*args, **kwargs):
    """Name of the DynamoDB table holding environment settings."""
    return _secrets_table_name(*args, **kwargs)


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


def _is_dynamodb_critical_exception(err):
    """Return true for botocore exceptions due to exceeded througput."""
    error_code = getattr(err, "response", {}).get("Error", {}).get("Code")
    return error_code in {"ProvisionedThroughputExceededException",
                          "UnrecognizedClientException"}


def _is_critical_exception(err):
    """True for CriticalException errors."""
    return isinstance(err, CriticalError)


def get_secret(key, environment=None, stage=None, namespace=None,
               wait_exponential_multiplier=50, wait_exponential_max=5000,
               stop_max_delay=10000):
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
    logger.info("Retriving key '{}' from table '{}'".format(
        key, table_name))

    @retry(retry_on_exception=_is_critical_exception,
           wait_exponential_multiplier=wait_exponential_multiplier,
           wait_exponential_max=wait_exponential_max,
           stop_max_delay=stop_max_delay)
    def get_item():
        try:
            return client.get_item(
                TableName=table_name,
                Key={'id': {'S': key}}).get('Item', {}).get(
                    'value', {}).get('B')
        except Exception as err:
            if _is_dynamodb_critical_exception(err):
                raise CriticalError(err)
            else:
                raise

    encrypted = get_item()

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


def get_environment_setting(*args, **kwargs):
    """Get environment setting."""
    return get_secret(*args, **kwargs)


def get_state(key, namespace=None, table_name=None, environment=None,
              layer=None, stage=None, shard_id=None, consistent=True,
              deserializer=json.loads, wait_exponential_multiplier=500,
              wait_exponential_max=5000, stop_max_delay=10000):
    """Get Lambda state value."""
    if table_name is None:
        table_name = _state_table_name(environment=environment, layer=layer,
                                       stage=stage)

    if not table_name:
        msg = ("Can't produce state table name: unable to get state "
               "item '{}'".format(key))
        logger.error(msg)
        raise StateTableError(msg)
        return

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    logger.info("Getting key '{}' from table '{}'".format(key, table_name))
    if namespace:
        key = "{}:{}".format(namespace, key)

    if shard_id:
        key = "{}:{}".format(shard_id, key)

    @retry(retry_on_exception=_is_critical_exception,
           wait_exponential_multiplier=wait_exponential_multiplier,
           wait_exponential_max=wait_exponential_max,
           stop_max_delay=stop_max_delay)
    def get_item():
        try:
            return table.get_item(
                Key={"id": key}, ConsistentRead=consistent).get(
                    "Item", {}).get("value")
        except Exception as err:
            if _is_dynamodb_critical_exception(err):
                raise CriticalError(err)
            else:
                raise

    value = get_item()

    if not value:
        return

    if deserializer:
        try:
            value = deserializer(value)
        except ValueError:
            # For backwards compatibility: plain strings are allowed
            logger.error("Unable to json-deserialize value '{}'".format(value))
            return value

    return value


def set_state(key, value, namespace=None, table_name=None, environment=None,
              layer=None, stage=None, shard_id=None, consistent=True,
              serializer=json.dumps, wait_exponential_multiplier=500,
              wait_exponential_max=5000, stop_max_delay=10000):
    """Set Lambda state value."""
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
    if serializer:
        try:
            value = serializer(value)
        except TypeError:
            logger.error(
                "Value for state key '{}' is not json-serializable".format(
                    key))
            raise

    if namespace:
        key = "{}:{}".format(namespace, key)

    if shard_id:
        key = "{}:{}".format(shard_id, key)

    @retry(retry_on_exception=_is_critical_exception,
           wait_exponential_multiplier=500,
           wait_exponential_max=5000,
           stop_max_delay=10000)
    def put_item():
        try:
            return table.put_item(Item={"id": key, "value": value})
        except Exception as err:
            if _is_dynamodb_critical_exception(err):
                raise CriticalError(err)
            else:
                raise

    resp = put_item()

    logger.info("Response from DynamoDB: '{}'".format(resp))
    return resp


def delete_state(key, namespace=None, table_name=None, environment=None,
                 layer=None, stage=None, shard_id=None, consistent=True,
                 wait_exponential_multiplier=500,
                 wait_exponential_max=5000, stop_max_delay=10000):
    """Delete Lambda state value."""
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
    logger.info("Deleting {} in DynamoDB table {}".format(key, table_name))

    if namespace:
        key = "{}:{}".format(namespace, key)

    if shard_id:
        key = "{}:{}".format(shard_id, key)

    @retry(retry_on_exception=_is_critical_exception,
           wait_exponential_multiplier=500,
           wait_exponential_max=5000,
           stop_max_delay=10000)
    def delete_item():
        try:
            return table.delete_item(Key={"id": key})
        except Exception as err:
            if _is_dynamodb_critical_exception(err):
                raise CriticalError(err)
            else:
                raise

    resp = delete_item()

    logger.info("Response from DynamoDB: '{}'".format(resp))
    return resp
