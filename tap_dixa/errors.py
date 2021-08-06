import requests

class DixaclientError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class DixaClient5xxError(DixaclientError):
    pass


class DixaClient401Error(DixaclientError):
    pass


class DixaClient400Error(DixaclientError):
    pass


class DixaClient429Error(DixaclientError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    500: {
        'raise_exception': DixaClient5xxError,
        'message': 'Server Error',
    },
    503: {
        'raise_exception': DixaClient5xxError,
        'message': 'Server Error',
    },
    401: {
        'raise_exception': DixaClient401Error,
        'message': 'Invalid or missing credentials'
    },
    400: {
        'raise_exception': DixaClient400Error,
        'message': 'Invalid query parameters'
    },
    429: {
        'raise_exception': DixaClient429Error,
        'message': 'API limit has been reached'
    },
}


def raise_for_error(resp):
    try:
        resp.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            error_code = resp.status_code
            client_exception = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {})
            exc = client_exception.get('raise_exception', DixaclientError)
            message = client_exception.get('message', 'Client Error')

            raise exc(message, resp) from None

        except (ValueError, TypeError):
            raise DixaclientError(error) from None
