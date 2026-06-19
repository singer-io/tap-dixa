"""
HTTP Error Code exception tests for tap_dixa
"""
from unittest import TestCase, mock
import requests
from tap_dixa.client import Client
import tap_dixa.exceptions as exceptions


class Mockresponse:
    def __init__(self, resp, status_code, content=[], headers=None, raise_error=True):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("sample message")


class HTTPErrorCodeHandling(TestCase):
    """
    Test cases to verify error is raised with proper message  for get_resource method.
    """

    def setUp(self):
        self.check_access_patcher = mock.patch.object(Client, "check_access", return_value=True)
        self.check_access_patcher.start()
        self.client_obj = Client("TEST")

    def tearDown(self):
        self.check_access_patcher.stop()

    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse("", 400))
    def test_400_error_custom_message(self, *args): 
        """
        Unit test to check proper error message for 400 status code.
        """
        with self.assertRaises(exceptions.DixaClient400Error):
            try:
                self.client_obj.get("https://test.com", "/test")
            except exceptions.DixaClientError as _:
                self.assertEqual(str(_), "Invalid query parameters")
                raise _

    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse("", 401))
    def test_401_error_custom_message(self, *args):
        """
        Unit test to check proper error message for 401 status code.
        """
        with self.assertRaises(exceptions.DixaClient401Error):
            try:
                self.client_obj.get("https://test.com", "/test")
            except exceptions.DixaClientError as _:
                self.assertEqual(str(_), "Invalid or missing credentials")
                raise _

    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse("", 408))
    def test_408_error_custom_message(self, *args):
        """
        Unit test to check proper error message for 408 status code.
        """
        with self.assertRaises(exceptions.DixaClient408Error):
            try:
                self.client_obj.get("https://test.com", "/test")
            except exceptions.DixaClientError as _:
                self.assertEqual(str(_), "Request Timeout")
                raise _

    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse("", 422))
    def test_422_error_custom_message(self, *args):
        """
        Unit test to check proper error message for 422 status code.
        """
        with self.assertRaises(exceptions.DixaClient422Error):
            try:
                self.client_obj.get("https://test.com", "/test")
            except exceptions.DixaClientError as _:
                self.assertEqual(str(_), "Exceeded max allowed 10 csids per request")
                raise _

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse("", 429))
    def test_429_error_custom_message(self, *args):
        """
        Unit test to check proper error message for 429 status code.
        """
        with self.assertRaises(exceptions.DixaClient429Error):
            try:
                self.client_obj.get("https://test.com", "/test")
            except exceptions.DixaClientError as _:
                self.assertEqual(str(_), "API limit has been reached")
                raise _

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse("", 500))
    def test_500_error_custom_message(self, *args):
        """
        Unit test to check proper error message for 500 status code.
        """
        with self.assertRaises(exceptions.DixaClient5xxError):
            try:
                self.client_obj.get("https://test.com", "/test")
            except exceptions.DixaClientError as _:
                self.assertEqual(str(_), "Dixa Server Error")
                raise _

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse("", 503))
    def test_503_error_custom_message(self, *args):
        """
        Unit test to check proper error message for 503 status code.
        """
        with self.assertRaises(exceptions.DixaClient5xxError):
            try:
                self.client_obj.get("https://test.com", "/test")
            except exceptions.DixaClientError as _:
                self.assertEqual(str(_), "Dixa Server Unavailable")
                raise _


class ClientCheckAccessInit(TestCase):
    """Tests for client initialization and check_access behavior."""

    @mock.patch.object(Client, "check_access", return_value=True)
    def test_init_calls_check_access(self, mock_check_access):
        Client("TEST")
        mock_check_access.assert_called_once()

    @mock.patch.object(Client, "get")
    def test_check_access_raises_for_invalid_credentials(self, mock_get):
        mock_get.side_effect = exceptions.DixaClient401Error("Unauthorized")
        client = Client.__new__(Client)
        client._api_token = "TEST"
        client._base_url = None
        client._session = mock.Mock()
        client._headers = {}

        with self.assertRaises(exceptions.DixaClient401Error):
            Client.check_access(client)

    @mock.patch.object(Client, "get")
    def test_check_access_reraises_non_auth_client_errors(self, mock_get):
        mock_get.side_effect = exceptions.DixaClient429Error("Rate limit")
        client = Client.__new__(Client)
        client._api_token = "TEST"
        client._base_url = None
        client._session = mock.Mock()
        client._headers = {}

        with self.assertRaises(exceptions.DixaClient429Error):
            Client.check_access(client)