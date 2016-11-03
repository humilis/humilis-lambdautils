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


def annotate_function(namespace=None):
    """Add input and output watermarks to processed events."""
    def decorator(func):
        """Func decorator to annotate events with entry and/or exit timestamps."""
        def wrapper(ev, *args, **kwargs):
            funcname = ":".join([func.__module__, func.__name__])
            key = _make_key(namespace, funcname + "|input")
            ev = annotate_event(ev, key)
            try:
                out = func(ev, *args, **kwargs)
            finally:
                key = _make_key(key, funcname + "|output")
                ev = annotate_event(ev, key)
            return out

        return wrapper
    return decorator


def annotate_event(ev, key, namespace=None):
    """Add an annotation to an event."""
    ann = {}
    ann["ts"] = time.time()
    ann["key"] = _make_key(namespace, key)
    _h = ev.get("_humilis", {})
    if not _h:
        ev["_humilis"] = {"annotation": [ann]}
    else:
        ev["_humilis"]["annotation"] = _h.get("annotation", [])
        ev["_humilis"]["annotation"].append(ann)

    return ev


def get_annotations(event, key, namespace=None):
    """Produce the list of annotations for a given key."""
    key = _make_key(namespace, key)
    return [ann for ann in event.get("_humilis", {}).get("annotation", [])
            if re.match(key, ann["key"])]


def get_function_annotations(event, funcname, type=None, namespace=None):
    """Produce a list of function annotations in in this event."""
    if type:
        postfix = "|" + type
    else:
        postfix = "|.+"
    return get_annotations(event, funcname + postfix, namespace)


def _make_key(namespace, key):
    """Build a namespaced annotation key."""
    if namespace:
        return "{}|{}".format(namespace, key)
    else:
        return key
