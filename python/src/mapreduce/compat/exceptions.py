"""Compatibility exceptions replacing App Engine SDK exception classes.

Maps old google.appengine exception types to cloud-native equivalents.
"""

from google.api_core import exceptions as api_exceptions


DatastoreError = api_exceptions.GoogleAPICallError

BadValueError = ValueError

DeadlineExceededError = api_exceptions.DeadlineExceeded

ApiError = api_exceptions.GoogleAPICallError


class RequestTooLargeError(ApiError):
    """Raised when an API request exceeds the maximum allowed size."""

    def __init__(self, message="Request too large"):
        super().__init__(message)


class Timeout(Exception):
    """Raised when a datastore operation times out."""

    def __init__(self, message="The datastore operation timed out"):
        super().__init__(message)


class Rollback(Exception):
    """Raised inside a transaction to trigger a rollback without error."""
