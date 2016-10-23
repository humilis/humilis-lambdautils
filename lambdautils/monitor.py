"""Monitoring utilities."""

import json
import logging
import os
import socket
import uuid

import raven
from raven.handlers.logging import SentryHandler

from .state import get_secret

from .kinesis import (unpack_kinesis_event, send_to_kinesis_stream,
                      send_to_delivery_stream)
from .exception import CriticalError


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


def _sentry_context_dict(context):
    """Create a dict with context information for Sentry."""
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


def sentry_monitor(error_stream=None, **kwargs):
    """Sentry monitoring for AWS Lambda handler."""
    def decorator(func):
        """A decorator that adds Sentry monitoring to a Lambda handler."""
        def wrapper(event, context):
            """Wrap the target function."""
            client = _setup_sentry_client(context)
            try:
                return func(event, context)
            except CriticalError:
                # Raise the exception and block the stream processor
                if client:
                    client.captureException()
                raise
            except Exception as err:
                try:
                    _handle_non_critical_exception(err, error_stream, event)
                except:
                    # If it could not be handled, it becomes critical
                    if client:
                        client.captureException()
                    raise
        return wrapper

    return decorator


def _setup_sentry_client(context):
    """Produce and configure the sentry client."""

    dsn = get_secret("sentry.dsn")
    try:
        client = raven.Client(dsn)
        handler = SentryHandler(client)
        logger.addHandler(handler)
        client.user_context(_sentry_context_dict(context))
        return client
    except:
        logger.error("Raven client error", exc_info=True)
        return None


def _handle_non_critical_exception(err, error_stream, event):
    """Deliver errors to error stream."""
    logger.error("AWS Lambda exception", exc_info=True)
    errevents = _make_error_events(event)
    logger.info("Error events: %s", json.dumps(errevents, indent=4))

    kinesis_stream = error_stream.get("kinesis_stream")
    if kinesis_stream:
        send_to_kinesis_stream(
            errevents,
            kinesis_stream,
            partition_key=error_stream.get("partition_key", str(uuid.uuid4())))
        logger.info("Sent payload to Kinesis stream '%s'", error_stream)

    delivery_stream = error_stream.get("firehose_delivery_stream")
    if delivery_stream:
        send_to_delivery_stream(errevents, delivery_stream)
        logger.info("Sent payload to Firehose delivery stream '%s'",
                    delivery_stream)

    if not kinesis_stream and not delivery_stream:
        # Promote to Critical exception
        raise err


def _make_error_events(event):
    """Make error events."""
    try:
        recs, _ = unpack_kinesis_event(event, deserializer=None)
    except KeyError:
        # If not a Kinesis event, just unpack the records
        recs = event["Records"]

    return [{
        "message_id": str(uuid.uuid4()),
        "schema_version": "1.0.0",
        "type": "error",
        "channel": "polku",
        "payload": rec} for rec in recs]
