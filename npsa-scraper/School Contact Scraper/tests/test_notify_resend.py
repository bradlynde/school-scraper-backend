"""
Resend (HTTP) notify unit tests — mocked, no real network.
Run: python3 -m unittest tests.test_notify_resend -v
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from external_services import notify  # noqa: E402


class TestNotifyResend(unittest.TestCase):
    _env = {
        "NOTIFY_ON_RUN_COMPLETE": "true",
        "RESEND_API_KEY": "re_test_key",
        "NOTIFY_EMAIL": "recipient@test.com",
        "NOTIFY_FROM": "onboarding@resend.dev",
    }

    @patch("external_services.notify.requests.post")
    def test_send_test_notification_posts_resend(self, mock_post: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.status_code = 200
        mock_resp.text = "{}"
        mock_post.return_value = mock_resp
        with patch.dict(os.environ, self._env, clear=False):
            r = notify.send_test_notification_email("School Scraper")
        self.assertEqual(r, {"ok": True})
        mock_post.assert_called_once()

    @patch("external_services.notify.requests.post")
    def test_send_run_complete_posts_resend(self, mock_post: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_post.return_value = mock_resp
        with patch.dict(os.environ, self._env, clear=False):
            notify.send_run_complete_email(
                "run-uuid",
                "arkansas",
                10,
                75,
                total_contacts=100,
                total_with_emails=40,
                duration_seconds=120.0,
            )
        body = mock_post.call_args[1]["json"]
        self.assertIn("School Scraper", body["subject"])

    @patch("external_services.notify.requests.post")
    def test_api_error_surfaces_in_test_helper(self, mock_post: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 422
        mock_resp.text = "invalid"
        mock_post.return_value = mock_resp
        with patch.dict(os.environ, self._env, clear=False):
            r = notify.send_test_notification_email("School Scraper")
        self.assertFalse(r["ok"])
        self.assertIn("422", r["error"])

    def test_disabled_when_notify_off(self) -> None:
        with patch.dict(os.environ, {"NOTIFY_ON_RUN_COMPLETE": "false"}, clear=False):
            r = notify.send_test_notification_email("School Scraper")
        self.assertFalse(r["ok"])


if __name__ == "__main__":
    unittest.main()
