""" Module providing DixaAPi Client"""
import base64
import backoff
import requests

from tap_dixa.exceptions import (DixaClient429Error, DixaClient408Error, 
                                DixaClient5xxError, raise_for_error,
                                retry_after_wait_gen)
from tap_dixa.helpers import DixaURL

class Client:
    """DixaClient Class for performing extraction from DixaApi"""

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
        message_bytes = message.encode("utf-8")
        base64_bytes = base64.b64encode(message_bytes)
        return base64_bytes.decode("utf-8")

    def _set_auth_header(self):
        """
        Sets the corresponding Authorization header based on the base url variant.
        """
        if self._base_url == DixaURL.EXPORTS.value:
            self._headers["Authorization"] = f"Basic {self._to_base64(self._api_token)}"
        elif self._base_url == DixaURL.INTEGRATIONS.value:
            self._headers["Authorization"] = f"{self._api_token}"

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
        return self._make_request(url, method="GET", headers=headers, params=params, data=data)

    def _post(self, url, headers=None, params=None, data=None):
        """
        Wraps the _make_request function with a 'POST' method
        """
        return self._make_request(url, method="POST", headers=headers, params=params, data=data)

    # Added retry logic for 3 times when bad request or server error or rate limit happens
    @backoff.on_exception(retry_after_wait_gen, (DixaClient429Error, DixaClient5xxError,DixaClient408Error), jitter=None, max_tries=3)
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
