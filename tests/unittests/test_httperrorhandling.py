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

    def raise_for_status(self): #noqa
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("sample message")


class HTTPErrorCodeHandling(TestCase):
    """
    Test cases to verify error is raised with proper message  for get_resource method.
    """

    client_obj = Client({"api_token": "TEST"})

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
    @mock.patch(
        "requests.Session.request", side_effect=lambda *_, **__: Mockresponse("", 500)
    )
    def test_500_error_custom_message(self, *args):
        """
        Unit test to check proper error message for 500 status code.
        """
        with self.assertRaises(exceptions.DixaClient5xxError):
            try:
                self.client_obj.get("https://test.com", "/test")
            except exceptions.DixaClientError as _:
                self.assertEqual(str(_), "Server Error")
                raise _

    @mock.patch("time.sleep")
    @mock.patch(
        "requests.Session.request", side_effect=lambda *_, **__: Mockresponse("", 503)
    )
    def test_503_error_custom_message(self, *args):
        """
        Unit test to check proper error message for 503 status code.
        """
        with self.assertRaises(exceptions.DixaClient5xxError):
            try:
                self.client_obj.get("https://test.com", "/test")
            except exceptions.DixaClientError as _:
                self.assertEqual(str(_), "Server Error")
                raise _
