"""Unit tests for lambdautils.exception."""

import pytest

import lambdautils


@pytest.mark.parametrize("exception", [
    lambdautils.utils.CriticalError,
    lambdautils.utils.StateTableError,
    lambdautils.utils.BadKinesisEventError,
    lambdautils.exception.RetryError])
def test_exceptions(exception):
    """Test the exceptions defined by lambdautils."""
    with pytest.raises(exception):
        raise exception("Nasty error")


@pytest.mark.parametrize("timeout", [1, 2, 3, 4])
def test_retry_error(timeout):
    """Test lambdautils.exception.RetryError."""

    try:
        raise lambdautils.exception.RetryError(timeout)
    except lambdautils.exception.RetryError as err:
        assert err.timeout == timeout
