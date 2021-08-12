import base64
from enum import Enum

import backoff
import requests
from singer import get_logger

LOGGER = get_logger()


class DixaClientError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class DixaClient5xxError(DixaClientError):
    pass


class DixaClient401Error(DixaClientError):
    pass


class DixaClient400Error(DixaClientError):
    pass


class DixaClient429Error(DixaClientError):
    pass


class DixaClient422Error(DixaClientError):
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
    422: {
        'raise_exception': DixaClient422Error,
        'message': 'Exceeded max allowed 10 csids per request'
    },
}


def raise_for_error(resp):
    try:
        resp.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            error_code = resp.status_code
            client_exception = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {})
            exc = client_exception.get('raise_exception', DixaClientError)
            message = client_exception.get('message', 'Client Error')

            raise exc(message, resp) from None

        except (ValueError, TypeError):
            raise DixaClientError(error) from None


def retry_after_wait_gen():
    while True:
        # This is called in an except block so we can retrieve the exception
        # and check it.
        sleep_time = 60
        LOGGER.info("API rate limit exceeded -- sleeping for %s seconds", sleep_time)
        yield sleep_time


class DixaURL(Enum):
    exports = 'https://exports.dixa.io'
    integrations = 'https://integrations.dixa.io'


class Client:

    def __init__(self, api_token: str):
        self._api_token = api_token
        self._base_url = None
        self._session = requests.Session()
        self._headers = {}

    @staticmethod
    def _to_base64(string: str):
        message = f"bearer:{string}"
        message_bytes = message.encode('utf-8')
        base64_bytes = base64.b64encode(message_bytes)
        return base64_bytes.decode('utf-8')

    def _set_auth_header(self):
        if self._base_url == DixaURL.exports.value:
            self._headers['Authorization'] = f"Basic {self._to_base64(self._api_token)}"

        if self._base_url == DixaURL.integrations.value:
            self._headers['Authorization'] = f"{self._api_token}"

    def _build_url(self, endpoint):
        return f"{self._base_url}{endpoint}"

    def _get(self, url, headers=None, params=None, data=None):
        return self._make_request(url, method='GET', headers=headers, params=params, data=data)

    def _post(self, url, headers=None, params=None, data=None):
        return self._make_request(url, method='POST', headers=headers, params=params, data=data)

    @backoff.on_exception(retry_after_wait_gen, DixaClient429Error, jitter=None, max_tries=3)
    def _make_request(self, url, method, headers=None, params=None, data=None):

        with self._session as session:
            response = session.request(method, url, headers=headers, params=params, data=data)

            if response.status_code != 200:
                raise_for_error(response)
                return None

            return response.json()

    def get(self, base_url, endpoint, params=None):
        self._base_url = base_url
        self._set_auth_header()
        url = self._build_url(endpoint)
        return self._get(url, headers=self._headers, params=params)
