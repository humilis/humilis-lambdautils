"""Exceptions."""


class CriticalError(Exception):

    """A blocking critical exception."""

    pass


class StateTableError(Exception):

    """Unable to retrieve the name of a processor state table."""

    pass
