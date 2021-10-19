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
            exc = client_exception.get('raise_exception', DixaClientError)
            message = client_exception.get('message', 'Client Error')

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


class DixaURL(Enum):
    """
    Enum representing the Dixa base url API variants.
    """
    exports = 'https://exports.dixa.io'
    integrations = 'https://dev.dixa.io'


class Client:

    def __init__(self, api_token: str):
        self._api_token = api_token
        self._base_url = None
        self._session = requests.Session()
        self._headers = {}

    @staticmethod
    def _to_base64(string: str) -> str:
        """
        Base64 encodes a string.

        :param string: String to be base64 encoded
        :return: Base64 encoded string
        """
        message = f"bearer:{string}"
        message_bytes = message.encode('utf-8')
        base64_bytes = base64.b64encode(message_bytes)
        return base64_bytes.decode('utf-8')

    def _set_auth_header(self):
        """
        Sets the corresponding Authorization header based on the base url variant.
        """
        if self._base_url == DixaURL.exports.value:
            self._headers['Authorization'] = f"Basic {self._to_base64(self._api_token)}"

        if self._base_url == DixaURL.integrations.value:
            self._headers['Authorization'] = f"{self._api_token}"

    def _build_url(self, endpoint: str) -> str:
        """
        Builds the URL for the API request.

        :param endpoint: The API URI (resource)
        :return: The full API URL for the request
        """
        return f"{self._base_url}{endpoint}"

    def _get(self, url, headers=None, params=None, data=None):
        """
        Wraps the _make_request function with a 'GET' method
        """
        return self._make_request(url, method='GET', headers=headers, params=params, data=data)

    def _post(self, url, headers=None, params=None, data=None):
        """
        Wraps the _make_request function with a 'POST' method
        """
        return self._make_request(url, method='POST', headers=headers, params=params, data=data)

    @backoff.on_exception(retry_after_wait_gen, DixaClient429Error, jitter=None, max_tries=3)
    def _make_request(self, url, method, headers=None, params=None, data=None) -> dict:
        """
        Makes the API request.

        :param url: The full API url
        :param method: The API request method
        :param headers: The headers for the API request
        :param params: The querystring params passed to the API
        :param data: The data passed to the body of the request
        :return: A dictionary representing the response from the API
        """

        with self._session as session:
            response = session.request(method, url, headers=headers, params=params, data=data)

            if response.status_code != 200:
                raise_for_error(response)
                return None

            return response.json()

    def get(self, base_url, endpoint, params=None):
        """
        Takes the base_url and endpoint and builds and makes a 'GET' request
        to the API.
        """
        self._base_url = base_url
        self._set_auth_header()
        url = self._build_url(endpoint)
        return self._get(url, headers=self._headers, params=params)
