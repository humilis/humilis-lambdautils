"""Exceptions."""


class CriticalError(Exception):

    """A blocking critical exception."""

    pass


class ProcessingError(Exception):

    """Error processing event."""

    def __init__(self, events, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.events = events


class StateTableError(Exception):

    """Unable to retrieve the name of a processor state table."""

    pass
