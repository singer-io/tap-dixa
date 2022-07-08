""" exceptions specific to dixa-client"""
import requests

from singer import get_logger

LOGGER = get_logger()


class InvalidInterval(Exception):
    pass


class DixaClientError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class DixaClient400Error(DixaClientError):
    pass

class DixaClient401Error(DixaClientError):
    pass

class DixaClient408Error(DixaClientError):
    pass

class DixaClient429Error(DixaClientError):
    pass


class DixaClient422Error(DixaClientError):
    pass

class DixaClient5xxError(DixaClientError):
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {"raise_exception": DixaClient400Error, "message": "Invalid query parameters"},
    401: {"raise_exception": DixaClient401Error, "message": "Invalid or missing credentials"},
    408: {"raise_exception": DixaClient408Error, "message": "Request Timeout"},
    422: {"raise_exception": DixaClient422Error, "message": "Exceeded max allowed 10 csids per request"},
    429: {"raise_exception": DixaClient429Error, "message": "API limit has been reached"},
    500: {"raise_exception": DixaClient5xxError,"message": "Dixa Server Error",},
    503: {"raise_exception": DixaClient5xxError,"message": "Dixa Server Unavailable",},
}


def raise_for_error(resp: requests.Response):
    """
    Raises the associated response exception.

    Takes in a response object, checks the status code, and throws the associated
    exception based on the status code.

    :param resp: requests.Response object
    """
    try:
        resp.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            error_code = resp.status_code
            client_exception = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {})
            exc = client_exception.get("raise_exception", DixaClientError)
            message = client_exception.get("message", "Client Error")

            raise exc(message, resp) from None

        except (ValueError, TypeError):
            raise DixaClientError(error) from None


def retry_after_wait_gen():
    """
    Returns a generator that is passed to backoff decorator to indicate how long
    to backoff for in seconds.
    """
    while True:
        sleep_time = 60
        LOGGER.info("API rate limit exceeded -- sleeping for %s seconds", sleep_time)
        yield sleep_time
