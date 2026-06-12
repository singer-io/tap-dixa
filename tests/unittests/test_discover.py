"""Unit tests for tap_dixa discover module and check_stream_access helper."""
import unittest
from unittest.mock import MagicMock, patch

from tap_dixa.exceptions import DixaClient401Error
from tap_dixa.discover import check_stream_access, _get_probe_params, discover
from tap_dixa.streams import STREAMS


# ---------------------------------------------------------------------------
# check_stream_access
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):
    """Tests for the merged check_stream_access function in tap_dixa.discover."""

    def _make_stream_class(self, tap_stream_id, base_url="https://dev.dixa.io", endpoint="/v1/test"):
        cls = MagicMock()
        cls.tap_stream_id = tap_stream_id
        cls.base_url = base_url
        cls.endpoint = endpoint
        return cls

    def test_returns_true_when_client_succeeds(self):
        client = MagicMock()
        stream_cls = self._make_stream_class("activity_logs", "https://dev.dixa.io", "/v1/conversations/activitylog")
        result = check_stream_access(client, stream_cls)
        self.assertTrue(result)
        client.get.assert_called_once()

    def test_returns_false_when_client_raises_401(self):
        client = MagicMock()
        client.get.side_effect = DixaClient401Error("Unauthorized")
        stream_cls = self._make_stream_class("activity_logs", "https://dev.dixa.io", "/v1/conversations/activitylog")
        result = check_stream_access(client, stream_cls)
        self.assertFalse(result)

    def test_reraises_non_auth_error(self):
        """Non-401 errors are re-raised — only DixaClient401Error is caught."""
        client = MagicMock()
        client.get.side_effect = RuntimeError("422 Unprocessable Entity")
        stream_cls = self._make_stream_class("conversations", "https://exports.dixa.io", "/v1/conversation_export")
        with self.assertRaises(RuntimeError):
            check_stream_access(client, stream_cls)


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
        with self.assertRaises(TypeError):
            discover(None)

    def test_raises_value_error_without_api_token(self):
        with self.assertRaises(KeyError):
            discover({})

    @patch("tap_dixa.discover.get_schemas")
    @patch("tap_dixa.discover.check_stream_access")
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
    @patch("tap_dixa.discover.check_stream_access")
    @patch("tap_dixa.discover.Client")
    def test_inaccessible_stream_excluded_from_catalog(
        self, mock_client_cls, mock_check_access, mock_get_schemas
    ):
        """Streams that fail the access check are excluded from the catalog."""
        all_streams = list(STREAMS.keys())
        blocked_stream = all_streams[0]
        accessible_streams = all_streams[1:]  # at least one accessible to avoid empty-catalog exception

        mock_get_schemas.return_value = (
            {name: {"type": "object", "properties": {}} for name in all_streams},
            {name: [{"metadata": {"table-key-properties": ["id"],
                                  "forced-replication-method": "INCREMENTAL",
                                  "valid-replication-keys": ["created_at"]},
                     "breadcrumb": []}] for name in all_streams},
        )
        mock_check_access.side_effect = lambda client, cls: cls.tap_stream_id != blocked_stream

        catalog = discover(self._VALID_CONFIG)
        returned_stream_names = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn(blocked_stream, returned_stream_names)
        self.assertEqual(returned_stream_names, set(accessible_streams))

    @patch("tap_dixa.discover.get_schemas")
    @patch("tap_dixa.discover.check_stream_access")
    @patch("tap_dixa.discover.Client")
    def test_all_inaccessible_raises_exception(
        self, mock_client_cls, mock_check_access, mock_get_schemas
    ):
        """When no streams are accessible, discover() raises an exception."""
        mock_get_schemas.return_value = (
            {name: {"type": "object", "properties": {}} for name in STREAMS},
            {name: [{"metadata": {"table-key-properties": ["id"],
                                  "forced-replication-method": "INCREMENTAL",
                                  "valid-replication-keys": ["created_at"]},
                     "breadcrumb": []}] for name in STREAMS},
        )
        mock_check_access.return_value = False

        with self.assertRaises(Exception) as ctx:
            discover(self._VALID_CONFIG)
        self.assertIn("No streams are accessible with the provided API token", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
