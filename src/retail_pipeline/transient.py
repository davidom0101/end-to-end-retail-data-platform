"""Identify AWS failures that can succeed on a later attempt."""

from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    IncompleteReadError,
    ReadTimeoutError,
    ResponseStreamingError,
)

_THROTTLE_CODES = {
    "ActiveSessionsExceededException",
    "ActiveStatementsExceededException",
    "RequestLimitExceeded",
    "SlowDown",
    "Throttling",
    "ThrottlingException",
    "TooManyRequestsException",
}
_SERVICE_CODES = {
    "InternalFailure",
    "InternalServerException",
    "InternalServerError",
    "RequestTimeout",
    "RequestTimeoutException",
    "ServiceUnavailable",
    "ServiceUnavailableException",
}


def is_aws_throttled(error: BaseException) -> bool:
    """Return whether AWS explicitly rejected the request due to capacity."""
    if not isinstance(error, ClientError):
        return False
    status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    code = error.response.get("Error", {}).get("Code")
    return status == 429 or code in _THROTTLE_CODES


def is_transient_aws_error(error: BaseException) -> bool:
    """Limit retries to transport, throttling, and temporary service failures."""
    if isinstance(error, ClientError):
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = error.response.get("Error", {}).get("Code")
        return (
            is_aws_throttled(error)
            or status in {500, 502, 503, 504}
            or code in _SERVICE_CODES
        )
    return isinstance(
        error,
        (
            ConnectionClosedError,
            ConnectTimeoutError,
            EndpointConnectionError,
            IncompleteReadError,
            ReadTimeoutError,
            ResponseStreamingError,
        ),
    )
