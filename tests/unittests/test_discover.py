"""Unit tests for tap_dixa discover module and check_stream_access helper."""
import unittest
from unittest.mock import MagicMock, patch

from tap_dixa.exceptions import DixaClient401Error
from tap_dixa.discover import check_stream_access, _check_stream_access, _get_probe_params, discover
from tap_dixa.streams import STREAMS


# ---------------------------------------------------------------------------
# check_stream_access (shared helper)
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):
    """Tests for the shared check_stream_access helper in tap_dixa.helpers."""

    def test_returns_true_when_probe_succeeds(self):
        """Probe callable executes without error → stream is accessible."""
        result = check_stream_access(
            probe_fn=lambda: None,
            auth_error_types=DixaClient401Error,
        )
        self.assertTrue(result)

    def test_returns_false_on_auth_error(self):
        """Auth error raised by probe → stream is not accessible, returns False."""
        def _raise():
            raise DixaClient401Error("Unauthorized")

        result = check_stream_access(
            probe_fn=_raise,
            auth_error_types=DixaClient401Error,
        )
        self.assertFalse(result)

    def test_re_raises_non_auth_error(self):
        """Non-auth error → exception propagates."""
        def _raise():
            raise ValueError("Unexpected")

        with self.assertRaises(ValueError):
            check_stream_access(
                probe_fn=_raise,
                auth_error_types=DixaClient401Error,
            )

    def test_accepts_tuple_of_auth_error_types(self):
        """auth_error_types can be a tuple of exception types."""
        class AuthA(Exception):
            pass
        class AuthB(Exception):
            pass

        for exc_cls in (AuthA, AuthB):
            with self.subTest(exc=exc_cls.__name__):
                result = check_stream_access(
                    probe_fn=lambda e=exc_cls: (_ for _ in ()).throw(e()),
                    auth_error_types=(AuthA, AuthB),
                )
                self.assertFalse(result)


# ---------------------------------------------------------------------------
# _check_stream_access (tap-specific wrapper)
# ---------------------------------------------------------------------------

class TestDixaCheckStreamAccess(unittest.TestCase):
    """Tests for the tap-dixa _check_stream_access wrapper in discover.py."""

    def _make_stream_class(self, tap_stream_id, base_url, endpoint):
        cls = MagicMock()
        cls.tap_stream_id = tap_stream_id
        cls.base_url = base_url
        cls.endpoint = endpoint
        return cls

    def test_returns_true_when_client_succeeds(self):
        client = MagicMock()
        stream_cls = self._make_stream_class("activity_logs", "https://dev.dixa.io", "/v1/conversations/activitylog")
        result = _check_stream_access(client, "activity_logs", stream_cls)
        self.assertTrue(result)

    def test_returns_false_when_client_raises_401(self):
        client = MagicMock()
        client.get.side_effect = DixaClient401Error("Unauthorized")
        stream_cls = self._make_stream_class("activity_logs", "https://dev.dixa.io", "/v1/conversations/activitylog")
        result = _check_stream_access(client, "activity_logs", stream_cls)
        self.assertFalse(result)

    def test_reraises_non_auth_error(self):
        """Non-auth errors (e.g. 422) propagate from _check_stream_access."""
        client = MagicMock()
        client.get.side_effect = RuntimeError("422 Unprocessable Entity")
        stream_cls = self._make_stream_class("conversations", "https://exports.dixa.io", "/v1/conversation_export")
        with self.assertRaises(RuntimeError):
            _check_stream_access(client, "conversations", stream_cls)


# ---------------------------------------------------------------------------
# _get_probe_params
# ---------------------------------------------------------------------------

class TestGetProbeParams(unittest.TestCase):
    """Tests for _get_probe_params to ensure each stream uses the correct param names."""

    def _make_stream_class(self, tap_stream_id):
        cls = MagicMock()
        cls.tap_stream_id = tap_stream_id
        return cls

    def test_activity_logs_uses_from_to_datetime(self):
        """activity_logs must use fromDatetime/toDatetime (RFC-3339 strings)."""
        params = _get_probe_params(self._make_stream_class("activity_logs"))
        self.assertIn("fromDatetime", params)
        self.assertIn("toDatetime", params)
        self.assertNotIn("created_after", params)
        self.assertNotIn("created_before", params)
        # Values should be strings (RFC-3339)
        self.assertIsInstance(params["fromDatetime"], str)
        self.assertIsInstance(params["toDatetime"], str)

    def test_conversations_uses_updated_after_before(self):
        """conversations must use updated_after/updated_before (unix-ms integers)."""
        params = _get_probe_params(self._make_stream_class("conversations"))
        self.assertIn("updated_after", params)
        self.assertIn("updated_before", params)
        self.assertNotIn("created_after", params)
        self.assertNotIn("created_before", params)
        # Values should be integers (unix-ms)
        self.assertIsInstance(params["updated_after"], int)
        self.assertIsInstance(params["updated_before"], int)

    def test_messages_uses_created_after_before(self):
        """messages must use created_after/created_before (unix-ms integers)."""
        params = _get_probe_params(self._make_stream_class("messages"))
        self.assertIn("created_after", params)
        self.assertIn("created_before", params)
        self.assertNotIn("fromDatetime", params)
        self.assertNotIn("toDatetime", params)
        # Values should be integers (unix-ms)
        self.assertIsInstance(params["created_after"], int)
        self.assertIsInstance(params["created_before"], int)


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
