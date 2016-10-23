"""Unit tests for lambdautils.exception."""

import pytest

import lambdautils


@pytest.mark.parametrize("exception", [
    lambdautils.utils.CriticalError,
    lambdautils.utils.StateTableError,
    lambdautils.utils.BadKinesisEventError])
def test_exceptions(exception):
    """Test the exceptions defined by lambdautils."""
    with pytest.raises(exception):
        raise exception("Nasty error")
