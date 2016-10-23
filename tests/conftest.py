"""Fixtures."""


import base64
from datetime import datetime
import json
from mock import Mock
import random
import uuid

import pytest


@pytest.fixture(scope="function", params=[1, 10, 50])
def search_events(request):
    """A list of one or more search events."""
    return [{
        "id": str(uuid.uuid4()),
        "search_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "client_id": str(uuid.uuid4())} for _ in range(request.param)]


@pytest.fixture(scope="function")
def kinesis_payloads(search_events):
    """A Kinesis payload containing one or more search events."""
    try:
        return [base64.encodestring(json.dumps(ev)) for ev in search_events]
    except TypeError:
        return [base64.encodestring(json.dumps(ev).encode())
                for ev in search_events]


@pytest.fixture(scope="function")
def kinesis_event(kinesis_payloads):
    """A Kinesis event that wraps a search event."""
    return {
        "Records": [
            {
                "eventID": "shardId-0000000:{}".format(random.randint(0, 1e9)),
                "kinesis": {
                    "data": payload,
                    "approximateArrivalTimestamp": 1462460164.694,
                    "kinesisSchemaVersion": "1.0",
                    "partitionKey": "2065967209.1462460056",
                    "sequenceNumber": "4956124400661703850297034189702"
                    },
                } for payload in kinesis_payloads
            ]
        }


@pytest.fixture(scope="session")
def context():
    """A dummy CF/Lambda context object."""

    class DummyContext:
        def __init__(self):
            self.function_name = 'dummy_name'
            self.function_version = 1
            self.invoked_function_arn = "arn"
            self.memory_limit_in_mb = 128
            self.aws_request_id = str(uuid.uuid4())
            self.log_group_name = "dummy_group"
            self.log_stream_name = "dummy_stream"
            self.identity = Mock(return_value=None)
            self.client_context = Mock(return_value=None)

        def get_remaining_Time_in_millis():
            return 100

    return DummyContext()


@pytest.fixture(scope="function")
def kms_client():
    """A mocked version of boto3 DynamoDB client."""
    mocked = Mock()
    mocked.decrypt = Mock(return_value={"Plaintext": b"dummy"})
    return mocked


@pytest.fixture(scope="function")
def kinesis_client():
    """A mocked version of boto3 Kinesis client."""
    mocked = Mock()
    ok_resp = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    mocked.put_records = Mock(return_value=ok_resp)
    mocked.put_record_batch = Mock(return_value=ok_resp)
    return mocked


@pytest.fixture(scope="function")
def dynamodb_resource():
    """A mocked version of boto3 DynamoDB resource."""
    mock_item = Mock()
    mock_item.value = "encrypted"
    mock_item.get = Mock(return_value=None)
    rv = {"Item": mock_item}
    mocked_table = Mock()
    mocked_table.get_item = Mock(return_value=rv)
    mocked = Mock()
    mocked.Table = Mock(return_value=mocked_table)
    return mocked


@pytest.fixture(scope="function")
def dynamodb_client():
    mocked = Mock()
    rv = {'Item': {'value': {'B': 'encrypted'}}}
    mocked.get_item = Mock(return_value=rv)
    mocked.decrypt = Mock(return_value={'Plaintext': b'dummy'})
    return mocked


@pytest.fixture(scope="function")
def boto3_client(kinesis_client, kms_client, dynamodb_client):
    """A mock for boto3.client."""
    def produce_client(name):
        return {"kinesis": kinesis_client, "kms": kms_client,
                "firehose": kinesis_client,
                "dynamodb": dynamodb_client}[name]

    mocked = Mock(side_effect=produce_client)
    return mocked


@pytest.fixture(scope="function")
def boto3_resource(dynamodb_resource):
    """A mock for boto3.resource."""
    def produce_resource(name):
        return {'dynamodb': dynamodb_resource}[name]

    mocked = Mock(side_effect=produce_resource)
    return mocked


@pytest.fixture(scope="function")
def raven_client():
    """A mock for raven.client."""
    return Mock()


@pytest.fixture(scope='session')
def cf_kinesis_event():
    """A sample CF create event."""
    return {
        "StackId": "arn:aws:cloudformation:us-west-2:XX/stack-name/guid",
        "ResponseURL": "http://pre-signed-S3-url-for-response",
        "ResourceProperties": {
            "StackName": "stack-name",
            "List": [
                "1",
                "2",
                "3"
                ]
            },
        "RequestType": "Create",
        "ResourceType": "Custom::TestResource",
        "RequestId": "unique id for this create request",
        "LogicalResourceId": "MyTestResource"
    }


@pytest.fixture(scope="session")
def cf_context():
    """A dummy CF context object."""

    class DummyContext:
        def __init__(self):
            self.function_name = "funcname"
            self.function_version = 1
            self.invoked_function_arn = 'arn'
            self.memory_limit_in_mb = 128
            self.aws_request_id = str(uuid.uuid4())
            self.log_group_name = 'dummy_group'
            self.log_stream_name = 'dummy_stream'
            self.identity = Mock(return_value=None)
            self.client_context = Mock(return_value=None)

        def get_remaining_Time_in_millis():
            return 100

    return DummyContext()


@pytest.fixture(autouse=True)
def lambdaenv(monkeypatch):
    """Set AWS Lambda environment variables."""
    monkeypatch.setenv("HUMILIS_ENVIRONMENT", "dummyenv")
    monkeypatch.setenv("HUMILIS_LAYER", "dummylayer")
    monkeypatch.setenv("HUMILIS_STAGE", "dummystage")
