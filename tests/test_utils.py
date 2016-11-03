"""Unit tests for lambdautils.utils."""

import copy
import json
from mock import Mock
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


def test_annotate_callable(monkeypatch):
    """Test annotate_callable function decorator."""

    @lambdautils.utils.annotate_callable()
    def mapper(ev, *args, **kwargs):
        return ev

    annev = mapper({})
    anns = annev["_humilis"]["annotation"]
    # One input and one output annotation
    assert len(anns) == 2
    # Annotation should be sorted by ts
    assert anns[1]["ts"] > anns[0]["ts"]
    # Check the annotation schema
    KEYS = {"ts", "key"}
    for ann in anns:
        assert not set(ann.keys()).symmetric_difference(KEYS)
        # All annotation properties must be populated
        assert None not in set(ann.values())
