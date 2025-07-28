from unittest.mock import patch
import unittest
import requests
from requests.exceptions import ChunkedEncodingError
from datetime import datetime

from tap_dixa.client import Client, DixaClient429Error
from tap_dixa.exceptions import DixaClient5xxError


class Mockresponse:
    def __init__(self, resp, status_code, content=[], headers=None, raise_error=False):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("sample message")

def mocked_failed_429_request(*args, **kwargs):
    return Mockresponse('', 429, headers={}, raise_error=True)

class Test_backoff(unittest.TestCase):

    @patch("time.sleep")
    @patch("requests.Session.request", side_effect=mocked_failed_429_request)
    def test_too_many_requests_429_error(self, mocked_send, mocked_sleep):
        client = Client(api_token="test")

        try:
            """
            Verifying if the custom exception 'DixaClient429Error' 
            is raised on receiving status code 429
            """
            _ = client.get("https://test.com", "/test")
        except DixaClient429Error as api_error:
            pass

            """
            Verifying the retry is happening thrice for the 429 exception
            """
        self.assertEqual(mocked_send.call_count, 3)

    @patch("requests.Session.request", side_effect=mocked_failed_429_request)
    def test_request_timeout_and_backoff(self, mock_send):
        """
        Check whether the request backoffs properly for get call for more than a minute for Server429Error.
        """
        mock_send.side_effect = DixaClient429Error
        client = Client(api_token="test")
        before_time = datetime.now()
        with self.assertRaises(DixaClient429Error):
            _ = client.get("https://test.com", "/test")
        after_time = datetime.now()
        # verify that the tap backoff for more than 60 seconds
        time_difference = (after_time - before_time).total_seconds()
        self.assertTrue(60 <= time_difference <= 121)

    @patch("time.sleep")
    @patch("requests.Session.request", side_effect= lambda *args, **kwargs : Mockresponse('', 500, headers={}, raise_error=True))
    def test_500_server_error(self, mocked_send, mocked_sleep):
        client = Client(api_token="test")

        try:
            """
            Verifying if the custom exception 'DixaClient5xxError' 
            is raised on receiving status code 500
            """
            _ = client.get("https://test.com", "/test")
        except DixaClient5xxError as api_error:
            pass

            """
            Verifying the retry is happening thrice for the 500 server error exception
            """
        self.assertEqual(mocked_send.call_count, 3)

    @patch('time.sleep')
    @patch('requests.Session.request', side_effect=ChunkedEncodingError)
    def test_chunkedEncodingError(self, mocked_request, mocked_sleep):
        """
        Verifying that ChunkedEncodingError triggers retries 3 times
        """
        client = Client(api_token="test")

        with self.assertRaises(ChunkedEncodingError):
            _ = client.get("https://test.com", "/test")

        # Ensure that the request retries 3 times on ChunkedEncodingError
        self.assertEqual(mocked_request.call_count, 3)
