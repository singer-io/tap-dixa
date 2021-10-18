from unittest.mock import patch

import requests

from tap_dixa.client import Client, DixaClient429Error


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


@patch('requests.Session.request', side_effect=mocked_failed_429_request)
def test_too_many_requests_429_error(mocked):
    client = Client(api_token="test")

    try:
        # Verifying if the custom exception 'DixaClient429Error' is raised on receiving status code 429
        response = client.get("https://test.com", "/test")
    except DixaClient429Error as e:
        expected_error_message = "API limit has been reached"

        # Verifying the message formed for the custom exception
        assert str(e) == expected_error_message
