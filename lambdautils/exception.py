"""Exceptions."""


class CriticalError(Exception):
    pass


class StateTableError(Exception):
    pass


class RetryError(Exception):
    """Exception to be thrown when an event should be retried."""
    def __init__(self, timeout=5, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.timeout = timeout
