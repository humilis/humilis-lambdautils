"""Unit tests for lambdautils.utils."""

import copy
import json
from mock import Mock
import re
import time
import uuid

import lambdautils.utils
import pytest


def test_in_aws_lambda(monkeypatch):
    """Test in_aws_lambda."""
    assert not lambdautils.utils.in_aws_lambda()
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "token")
    assert lambdautils.utils.in_aws_lambda()


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


@pytest.mark.parametrize("ev", [
    ({}),
    ({"_humilis": {"annotation": []}}),
    ({"_humilis": {"annotation": [{"key": str(uuid.uuid4())}]}})])
def test_annotate_event(ev, monkeypatch):
    """Test adding an annotation to an event."""
    for envvar in ["HUMILIS_" + v for v in ["ENVIRONMENT", "STAGE", "LAYER"]]:
        monkeypatch.setenv(envvar, str(uuid.uuid4()))

    key = str(uuid.uuid4())
    # The annotations that are already present in the input event
    origanns = copy.copy(ev.get("_humilis", {}).get("annotation", []))
    annev = lambdautils.utils.annotate_event(ev, key)
    anns = annev.get("_humilis", {}).get("annotation", [])
    # Check that the new annotation was added
    assert key in {ann["key"] for ann in anns}
    # Check that the original annotations are still there and in the same order
    counter = 0
    for origann in origanns:
        for k, v in origann.items():
            assert anns[counter][k] == v
            counter += 1


def test_strip_annotations():
    """Test strip_annotations."""
    ev = lambdautils.utils.annotate_event({}, "key", "value")
    assert len(lambdautils.utils.get_annotations(ev, "key")) == 1
    assert "_humilis" in ev
    lambdautils.utils.strip_annotations(ev)
    assert len(lambdautils.utils.get_annotations(ev, "key")) == 0
    assert "_humilis" not in ev


def test_annotate_mapper():
    """Test annotate_mapper function decorator."""

    @lambdautils.utils.annotate_mapper()
    def mapper(event, *args, **kwargs):
        """A dummy mapper."""
        return event

    annev = mapper({})
    anns = annev["_humilis"]["annotation"]
    # One input and one output annotation
    assert len(anns) == 2
    # Annotation should be sorted by ts
    assert anns[1]["ts"] > anns[0]["ts"]
    # Check the annotation schema
    keys = {"ts", "key", "namespace"}
    for ann in anns:
        assert not set(ann.keys()).symmetric_difference(keys)
        # All annotation properties must be populated
        assert None not in set(ann.values())


def test_annotate_filter():
    """Test annotate_filter function decorator."""

    @lambdautils.utils.annotate_filter()
    def dummy_filter(event, *args, **kwargs):
        """A dummy filter."""
        return True

    annev = {}
    dummy_filter(annev)
    anns = annev["_humilis"]["annotation"]
    # One input and one output annotation
    assert len(anns) == 2
    # Annotation should be sorted by ts
    assert anns[1]["ts"] > anns[0]["ts"]
    # Check the annotation schema
    keys = {"ts", "key", "namespace"}
    for ann in anns:
        assert not set(ann.keys()).symmetric_difference(keys)
        # All annotation properties must be populated
        assert None not in set(ann.values())


def test_get_function_annotations():
    """Test annotate_mapper function decorator."""

    @lambdautils.utils.annotate_mapper()
    def mapper(event, *args, **kwargs):
        """A dummy mapper."""
        return event

    annev = mapper({})
    annev = mapper(annev)

    anns = lambdautils.utils.get_function_annotations(
        annev, "test_utils:mapper")
    # The second mapper call should have overwritten the original annotation
    assert len(anns) == 2


def test_annotate_error():
    """Test annotating an error."""
    error = KeyError(1)
    annev = lambdautils.utils.annotate_error({}, error)
    anns = lambdautils.utils.get_annotations(annev, error)
    assert len(anns) == 1


def test_expired_error():
    """Test checking for the expiration of an error."""
    error = KeyError(1)
    annev = lambdautils.utils.annotate_error({}, error)
    time.sleep(0.1)
    assert lambdautils.utils.error_has_expired(annev, error, 0.05)
    assert not lambdautils.utils.error_has_expired(annev, error, 100)
    assert not lambdautils.utils.error_has_expired(annev, ValueError(1), 0.05)
