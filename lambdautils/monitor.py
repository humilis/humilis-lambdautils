"""Monitoring utilities."""

import json
import logging
import operator
import os
import six
import uuid

import raven
from raven.handlers.logging import SentryHandler

from .state import get_secret

from .kinesis import (unpack_kinesis_event, send_to_kinesis_stream,
                      send_to_delivery_stream)
from .exception import CriticalError, ProcessingError, OutOfOrderError

# The AWS Lambda logger
rlogger = logging.getLogger()
rlogger.setLevel(logging.INFO)


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
            except (ProcessingError, OutOfOrderError) as err:
                # A controlled exception from the Kinesis processor
                _handle_processing_error(err, error_stream, client)
            except Exception as err:
                # Raise the exception and block the stream processor
                if client:
                    client.captureException()
                raise
        return wrapper

    return decorator


def _setup_sentry_client(context):
    """Produce and configure the sentry client."""

    # get_secret will be deprecated soon
    dsn = os.environ.get("SENTRY_DSN") or get_secret("sentry.dsn")
    try:
        client = raven.Client(dsn)
        client.user_context(_sentry_context_dict(context))
        return client
    except:
        rlogger.error("Raven client error", exc_info=True)
        return None


def _handle_processing_error(err, errstream, client):
    """Handle ProcessingError exceptions."""

    errors = sorted(err.events, key=operator.attrgetter("index"))
    failed = [e.event for e in errors]
    silent = all(isinstance(e.error, OutOfOrderError) for e in errors)
    if errstream:
        _deliver_errored_events(errstream, failed)
        must_raise = False
    else:
        must_raise = True
    for _, event, error, tb in errors:
        if isinstance(error, OutOfOrderError):
            # Not really an error: do not log this to Sentry
            continue
        try:
            raise six.reraise(*tb)
        except Exception as err:
            if client:
                client.captureException()
            msg = "{}{}: {}".format(type(err).__name__,
                    err.args, json.dumps(event, indent=4))
            rlogger.error(msg, exc_info=tb)
            if must_raise:
                raise


def _deliver_errored_events(errstream, recs):
    """Deliver errors to error stream."""
    rlogger.info("Going to handle %s failed events", len(recs))
    rlogger.info(
        "First failed event: %s", json.dumps(recs[0], indent=4))

    kinesis_stream = errstream.get("kinesis_stream")
    randomkey = str(uuid.uuid4())
    if kinesis_stream:
        send_to_kinesis_stream(
            recs,
            kinesis_stream,
            partition_key=errstream.get("partition_key", randomkey))
        rlogger.info("Sent errors to Kinesis stream '%s'", errstream)

    delivery_stream = errstream.get("firehose_delivery_stream")
    if delivery_stream:
        send_to_delivery_stream(errevents, delivery_stream)
        rlogger.info("Sent error payload to Firehose delivery stream '%s'",
                     delivery_stream)


def _get_records(event):
    """Get records from an AWS Lambda trigger event."""
    try:
        recs, _ = unpack_kinesis_event(event, deserializer=None)
    except KeyError:
        # If not a Kinesis event, just unpack the records
        recs = event["Records"]
    return recs
