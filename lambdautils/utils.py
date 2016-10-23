"""Utilities for Lambda functions."""

import json
import os
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


def annotate_callable(func):
    """Func decorator to annotate events with entry and/or exit timestamps."""
    def wrapper(ev, *args, **kwargs):
        funcname = ":".join([func.__module__, func.__name__])
        ann_key = funcname + "|input"
        ev = annotate_event(ev, ann_key)
        ev = func(ev, *args, **kwargs)
        ann_key = funcname + "|output"
        ev = annotate_event(ev, ann_key)
        return ev

    return wrapper


def annotate_event(ev, key):
    ann = {}
    ann["environment"] = os.environ.get("HUMILIS_ENVIRONMENT")
    ann["stage"] = os.environ.get("HUMILIS_STAGE")
    ann["layer"] = os.environ.get("HUMILIS_LAYER")
    ann["ts"] = time.time()
    ann["key"] = key
    _h = ev.get("_humilis", {})
    if not _h:
        ev["_humilis"] = {"annotation": [ann]}
    else:
        ev["_humilis"]["annotation"] = _h.get("annotation", [])
        ev["_humilis"]["annotation"].append(ann)

    return ev


