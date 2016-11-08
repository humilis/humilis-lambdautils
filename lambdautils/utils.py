"""Utilities for Lambda functions."""

import json
import os
import re
import time

try:
    from urllib2 import build_opener, Request, HTTPHandler, HTTPError
except ImportError:
    # We are in Python 3.x
    from urllib.request import build_opener, Request, HTTPHandler
    from urllib.error import HTTPError

# interface imports (for backwards compatibility)
from .monitor import sentry_monitor, CriticalError  # noqa
from .state import (get_secret, get_state, set_state, delete_state,  # noqa
                    StateTableError)  # noqa
from .kinesis import (unpack_kinesis_event, send_to_delivery_stream,  # noqa
                      send_to_kinesis_stream, BadKinesisEventError)  # noqa


def in_aws_lambda():
    """Returns true if running in AWS Lambda service."""
    return "AWS_SESSION_TOKEN" in os.environ \
        and "AWS_SESSION_TOKEN" in os.environ


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

    opener = build_opener(HTTPHandler)
    request = Request(event["ResponseURL"], data=response_body)
    request.add_header("Content-Type", "")
    request.add_header("Content-Length", len(response_body))
    request.get_method = lambda: 'PUT'
    try:
        response = opener.open(request)
        print("Status code: {}".format(response.getcode()))
        print("Status message: {}".format(response.msg))
        return True
    except HTTPError as exc:
        print("Failed executing HTTP request: {}".format(exc.code))
        return False


def annotate_function(**decargs):
    """Add input and output watermarks to processed events."""
    def decorator(func):
        """Annotate events with entry and/or exit timestamps."""
        def wrapper(event, *args, **kwargs):
            """Add enter and exit annotations to the processed event."""
            funcname = ":".join([func.__module__, func.__name__])
            enter_ts = time.time()
            out = func(event, *args, **kwargs)
            enter_key = funcname + "|enter"
            out = annotate_event(out, enter_key, ts=enter_ts, **decargs)
            exit_key = funcname + "|exit"
            out = annotate_event(out, exit_key, ts=time.time(), **decargs)
            return out

        return wrapper
    return decorator


def annotate_event(ev, key, ts=None, namespace=None, **kwargs):
    """Add an annotation to an event."""
    ann = {}
    if ts is None:
        ts = time.time()
    ann["ts"] = ts
    ann["key"] = key
    if namespace is None and "HUMILIS_ENVIRONMENT" in os.environ:
        namespace = "{}:{}:{}".format(
            os.environ.get("HUMILIS_ENVIRONMENT"),
            os.environ.get("HUMILIS_LAYER"),
            os.environ.get("HUMILIS_STAGE"))

    if namespace is not None:
        ann["namespace"] = namespace
    ann.update(kwargs)
    _humilis = ev.get("_humilis", {})
    if not _humilis:
        ev["_humilis"] = {"annotation": [ann]}
    else:
        ev["_humilis"]["annotation"] = _humilis.get("annotation", [])
        ev["_humilis"]["annotation"].append(ann)

    return ev


def get_annotations(event, key, namespace=None):
    """Produce the list of annotations for a given key."""
    return [ann for ann in event.get("_humilis", {}).get("annotation", [])
            if (re.match(key, ann["key"]) and
                (namespace is None or ann.get("namespace") == namespace))]


def get_function_annotations(event, funcname, type=None, namespace=None):
    """Produce a list of function annotations in in this event."""
    if type:
        postfix = "|" + type
    else:
        postfix = "|.+"
    return get_annotations(event, funcname + postfix, namespace)
