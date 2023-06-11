"""
Exception documentation. Note: all exceptions from the package inherit from the XTDBException class.
"""


class XTDBException(Exception):
    """Base exception for XTDB errors."""


class InvalidField(XTDBException):
    """Exception indicating an invalid field was passed during query creation in the ORM."""
