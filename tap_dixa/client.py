import base64
from enum import Enum

from requests import Session

from tap_dixa.errors import raise_for_error


class DixaURL(Enum):
    exports = 'https://exports.dixa.io'
    integrations = 'https://integrations.dixa.io'


class Client:

    def __init__(self, api_token: str):
        self._api_token = api_token
        self._base_url = None
        self._session = Session()
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

    def _make_request(self, url, method, headers=None, params=None, data=None):

        with self._session as session:
            print(f"headers: {self._headers}")
            response = session.request(method, url, headers=headers, params=params, data=data)

            if response.status_code != 200:
                # raise_for_error(response)
                # return None
                response.raise_for_status()

            return response.json()

    def get_conversations(self, params):
        self._base_url = DixaURL.exports.value
        self._set_auth_header()
        url = self._build_url('/v1/conversation_export')
        return self._get(url, headers=self._headers, params=params)

    def get_acitivity_logs(self, params):
        self._base_url = DixaURL.integrations.value
        self._set_auth_header()
        url = self._build_url('/v1/conversations/activitylog')
        return self._get(url, headers=self._headers, params=params)
