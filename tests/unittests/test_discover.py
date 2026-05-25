"""Unit tests for tap_dixa discover module and check_stream_access helper."""
import unittest
from unittest.mock import MagicMock, patch

from tap_dixa.exceptions import DixaClient401Error
from tap_dixa.helpers import check_stream_access
from tap_dixa.discover import _check_stream_access, discover
from tap_dixa.streams import STREAMS


# ---------------------------------------------------------------------------
# check_stream_access (shared helper)
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):
    """Tests for the shared check_stream_access helper in tap_dixa.helpers."""

    def test_returns_true_when_probe_succeeds(self):
        """Probe callable executes without error → stream is accessible."""
        result = check_stream_access(
            "my_stream",
            probe_fn=lambda: None,
            auth_error_types=DixaClient401Error,
        )
        self.assertTrue(result)

    def test_returns_false_on_auth_error(self):
        """Auth error raised by probe → stream is not accessible, returns False."""
        def _raise():
            raise DixaClient401Error("Unauthorized")

        result = check_stream_access(
            "my_stream",
            probe_fn=_raise,
            auth_error_types=DixaClient401Error,
        )
        self.assertFalse(result)

    def test_re_raises_non_auth_error_when_fallback_false(self):
        """Non-auth error + fallback_accessible=False → exception propagates."""
        def _raise():
            raise ValueError("Unexpected")

        with self.assertRaises(ValueError):
            check_stream_access(
                "my_stream",
                probe_fn=_raise,
                auth_error_types=DixaClient401Error,
                fallback_accessible=False,
            )

    def test_returns_true_on_non_auth_error_when_fallback_true(self):
        """Non-auth error (e.g. 400/422) + fallback_accessible=True → True (auth OK)."""
        def _raise():
            raise RuntimeError("400 Bad Request")

        result = check_stream_access(
            "my_stream",
            probe_fn=_raise,
            auth_error_types=DixaClient401Error,
            fallback_accessible=True,
        )
        self.assertTrue(result)

    def test_accepts_tuple_of_auth_error_types(self):
        """auth_error_types can be a tuple of exception types."""
        class AuthA(Exception):
            pass
        class AuthB(Exception):
            pass

        for exc_cls in (AuthA, AuthB):
            with self.subTest(exc=exc_cls.__name__):
                result = check_stream_access(
                    "my_stream",
                    probe_fn=lambda e=exc_cls: (_ for _ in ()).throw(e()),
                    auth_error_types=(AuthA, AuthB),
                )
                self.assertFalse(result)


# ---------------------------------------------------------------------------
# _check_stream_access (tap-specific wrapper)
# ---------------------------------------------------------------------------

class TestDixaCheckStreamAccess(unittest.TestCase):
    """Tests for the tap-dixa _check_stream_access wrapper in discover.py."""

    def _make_stream_class(self, base_url, endpoint):
        cls = MagicMock()
        cls.base_url = base_url
        cls.endpoint = endpoint
        return cls

    def test_returns_true_when_client_succeeds(self):
        client = MagicMock()
        stream_cls = self._make_stream_class("https://dev.dixa.io", "/v1/conversations/activitylog")
        result = _check_stream_access(client, "activity_logs", stream_cls)
        self.assertTrue(result)

    def test_returns_false_when_client_raises_401(self):
        client = MagicMock()
        client.get.side_effect = DixaClient401Error("Unauthorized")
        stream_cls = self._make_stream_class("https://dev.dixa.io", "/v1/conversations/activitylog")
        result = _check_stream_access(client, "activity_logs", stream_cls)
        self.assertFalse(result)

    def test_returns_true_when_client_raises_other_error(self):
        """400/422 from probe params means endpoint is reachable (fallback_accessible=True)."""
        client = MagicMock()
        client.get.side_effect = RuntimeError("422 Unprocessable Entity")
        stream_cls = self._make_stream_class("https://exports.dixa.io", "/v1/conversation_export")
        result = _check_stream_access(client, "conversations", stream_cls)
        self.assertTrue(result)


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):
    """Tests for the discover() function in tap_dixa.discover."""

    _VALID_CONFIG = {"api_token": "test-token"}

    def test_raises_value_error_without_config(self):
        with self.assertRaises(ValueError):
            discover(None)

    def test_raises_value_error_without_api_token(self):
        with self.assertRaises(ValueError):
            discover({})

    @patch("tap_dixa.discover.get_schemas")
    @patch("tap_dixa.discover._check_stream_access")
    @patch("tap_dixa.discover.Client")
    def test_all_accessible_streams_included_in_catalog(
        self, mock_client_cls, mock_check_access, mock_get_schemas
    ):
        """All streams pass access check → all appear in the catalog."""
        mock_get_schemas.return_value = (
            {name: {"type": "object", "properties": {}} for name in STREAMS},
            {name: [{"metadata": {"table-key-properties": ["id"],
                                  "forced-replication-method": "INCREMENTAL",
                                  "valid-replication-keys": ["created_at"]},
                     "breadcrumb": []}] for name in STREAMS},
        )
        mock_check_access.return_value = True

        catalog = discover(self._VALID_CONFIG)
        returned_stream_names = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(returned_stream_names, set(STREAMS.keys()))

    @patch("tap_dixa.discover.get_schemas")
    @patch("tap_dixa.discover._check_stream_access")
    @patch("tap_dixa.discover.Client")
    def test_inaccessible_stream_excluded_from_catalog(
        self, mock_client_cls, mock_check_access, mock_get_schemas
    ):
        """Streams that fail the access check are excluded from the catalog."""
        all_streams = list(STREAMS.keys())
        blocked_stream = all_streams[0]

        mock_get_schemas.return_value = (
            {name: {"type": "object", "properties": {}} for name in all_streams},
            {name: [{"metadata": {"table-key-properties": ["id"],
                                  "forced-replication-method": "INCREMENTAL",
                                  "valid-replication-keys": ["created_at"]},
                     "breadcrumb": []}] for name in all_streams},
        )
        mock_check_access.side_effect = lambda client, name, cls: name != blocked_stream

        catalog = discover(self._VALID_CONFIG)
        returned_stream_names = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn(blocked_stream, returned_stream_names)
        self.assertEqual(returned_stream_names, set(all_streams) - {blocked_stream})

    @patch("tap_dixa.discover.get_schemas")
    @patch("tap_dixa.discover._check_stream_access")
    @patch("tap_dixa.discover.Client")
    def test_all_inaccessible_returns_empty_catalog(
        self, mock_client_cls, mock_check_access, mock_get_schemas
    ):
        """When no streams are accessible, catalog is empty."""
        mock_get_schemas.return_value = (
            {name: {"type": "object", "properties": {}} for name in STREAMS},
            {name: [{"metadata": {"table-key-properties": ["id"],
                                  "forced-replication-method": "INCREMENTAL",
                                  "valid-replication-keys": ["created_at"]},
                     "breadcrumb": []}] for name in STREAMS},
        )
        mock_check_access.return_value = False

        catalog = discover(self._VALID_CONFIG)
        self.assertEqual(catalog.streams, [])


if __name__ == "__main__":
    unittest.main()
