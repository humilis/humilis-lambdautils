# -*- coding: utf-8 -*-
"""Basic unit tests."""

import inspect
import os
import sys

# Add the lambda directory to the python library search path
lambda_dir = os.path.join(
    os.path.dirname(inspect.getfile(inspect.currentframe())), '..')
sys.path.append(lambda_dir)

import lambdautils.utils as utils


def test_get_secret(boto3_resource, boto3_client, monkeypatch):
    """Gets a secret from DynamoDB."""
    # Call to the DynamoDB client to retrieve the encrypted secret
    monkeypatch.setattr("boto3.resource", boto3_resource)
    monkeypatch.setattr("boto3.client", boto3_client)
    utils.get_secret("sample_secret")
    boto3_client("dynamodb").get_item.assert_called_with(
        TableName=utils.SECRETS_TABLE_NAME,
        Key={"id": {"S": "sample_secret"}})

    # Call to the KMS client to decrypt the secret
    boto3_client('kms').decrypt.assert_called_with(CiphertextBlob="encrypted")


def test_get_state(boto3_resource, monkeypatch):
    """Gets a state value from DynamoDB."""
    monkeypatch.setattr("boto3.resource", boto3_resource)
    utils.get_state("sample_state_key")
    boto3_resource("dynamodb").Table().get_item.assert_called_with(
        Key={"id": "sample_state_key"})
