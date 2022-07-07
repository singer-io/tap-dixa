from unittest.mock import patch
import unittest
import requests

from tap_dixa.client import Client, DixaClient429Error
from tap_dixa.exceptions import DixaClient400Error, DixaClient422Error


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

@patch("time.sleep")
@patch("requests.Session.request", side_effect=mocked_failed_429_request)
class Test_Client(unittest.TestCase):

    def test_too_many_requests_429_error(self, mocked_send, mocked_sleep):
        client = Client(api_token="test")

        try:
            # Verifying if the custom exception 'DixaClient429Error' is raised on receiving status code 429
            _ = client.get("https://test.com", "/test")
        except DixaClient429Error: #as api_error:
            pass

            # Verifying the retry is happening thrice for the 429 exception
        self.assertEquals(mocked_send.call_count, 3)
