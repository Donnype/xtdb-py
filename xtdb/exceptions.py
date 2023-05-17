class XTDBException(Exception):
    """Base exception for XTDB errors."""


class InvalidField(XTDBException):
    pass


class InvalidPath(XTDBException):
    pass
