"""Monitoring utilities."""

import json
import logging
import os
import socket
import traceback

import raven

from .state import get_secret

from .kinesis import (unpack_kinesis_event, send_to_kinesis_stream,
                      send_to_delivery_stream)


logger = logging.getLogger()
logger.setLevel(logging.INFO)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

GRAPHITE_HOST = "statsd.hostedgraphite.com"
GRAPHITE_PORT = 8125


class CriticalError(Exception):
    def __init__(self, exception):
        self.__exception = exception

    def __str__(self):
        return str(self.__exception)


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


def sentry_monitor(environment=None, stage=None, layer=None,
                   error_stream=None, sentry_key="sentry.dsn"):
    """Monitor a function with Sentry."""
    if environment is None:
        environment = os.environ.get("HUMILIS_ENVIRONMENT")

    if stage is None:
        stage = os.environ.get("HUMILIS_STAGE")

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
            dsn = get_secret(sentry_key,
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
                client.user_context(_sentry_context_dict(context))

            try:
                return func(event, context)
            except CriticalError:
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


def graphite_monitor(metric, environment=None, stage=None,
                     graphite_key="graphite.api_key",
                     counter=lambda ret: int(bool(ret))):
    """Monitor a callable with Graphite."""

    if environment is None:
        environment = os.environ.get("HUMILIS_ENVIRONMENT")

    if stage is None:
        stage = os.environ.get("HUMILIS_STAGE")

    if not getattr(graphite_monitor, "api_key", None):
        graphite_monitor.api_key = get_secret(
            graphite_key, environment=environment, stage=stage)

    def decorator(func):
        def wrapper(*args, **kwargs):
            val = func(*args, **kwargs)
            if graphite_monitor.api_key:
                sock.sendto("{api_key}.{metric} {val}\n".format(
                    api_key=graphite_monitor.api_key, metric=metric,
                    val=str(counter(val))),
                    (GRAPHITE_HOST, GRAPHITE_PORT))
            return val

        return wrapper

    return decorator
