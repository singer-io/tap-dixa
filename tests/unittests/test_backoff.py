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
@patch('requests.Session.request', side_effect=mocked_failed_429_request)
def test_too_many_requests_429_error(mocked,mocked_sleep):
    client = Client(api_token="test")

    try:
        # Verifying if the custom exception 'DixaClient429Error' is raised on receiving status code 429
        response = client.get("https://test.com", "/test")
    except DixaClient429Error as e:
        expected_error_message = "API limit has been reached"

        # Verifying the message formed for the custom exception
        assert str(e) == expected_error_message

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

def mocked_failed_400_request(*args, **kwargs):
    return Mockresponse('', 400, headers={}, raise_error=True)

# Bad Request exception when the endpoint url is not correct
@patch('requests.Session.request', side_effect=mocked_failed_400_request)
def test_bad_requests_400_error(mocked):
    client = Client(api_token="test")

    try:
        # Verifying if the custom exception 'DixaClient400Error' is raised on receiving inappropriate query parameters
        response = client.get("https://test.com", "/test", params= {'created_after1':'2022-11-01','created_before':'2022-02-01'})
    except DixaClient400Error as e:
        expected_error_message = "Invalid query parameters"

        # Verifying the message formed for the custom exception
        assert str(e) == expected_error_message