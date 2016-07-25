"""Exceptions."""


class CriticalError(Exception):
    def __init__(self, exception):
        self.__exception = exception

    def __str__(self):
        return str(self.__exception)
