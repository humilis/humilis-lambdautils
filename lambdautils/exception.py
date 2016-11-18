"""Exceptions."""


class CriticalError(Exception):

    """A blocking critical exception."""

    pass


class ContextError(Exception):

    """Error retrieving context for event."""

    pass


class NoParentError(Exception):

    """An event parent context could not be retrieved."""

    pass


class ProcessingError(Exception):

    """Error processing event."""

    def __init__(self, events, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.events = events


class OutOfOrderError(Exception):

    """An event is out of order: not really an error."""

    pass


class StateTableError(Exception):

    """Unable to retrieve the name of a processor state table."""

    pass
