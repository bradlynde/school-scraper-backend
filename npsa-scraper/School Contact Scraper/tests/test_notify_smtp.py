"""
SMTP notify unit tests (mocked — no real network or email).
Run from School Contact Scraper directory:

  python3 -m unittest tests.test_notify_smtp -v
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


class TestNotifySmtp(unittest.TestCase):
    _smtp_env = {
        "NOTIFY_ON_RUN_COMPLETE": "true",
        "SMTP_HOST": "smtp.example.com",
        "SMTP_PORT": "587",
        "SMTP_USER": "user@test.com",
        "SMTP_PASSWORD": "secret",
        "NOTIFY_EMAIL": "recipient@test.com",
    }

    @patch("external_services.notify.smtplib.SMTP")
    def test_send_test_notification_calls_smtp(self, _mock_smtp_class: MagicMock) -> None:
        mock_server = MagicMock()
        _mock_smtp_class.return_value.__enter__.return_value = mock_server
        with patch.dict(os.environ, self._smtp_env, clear=False):
            r = notify.send_test_notification_email("School Scraper")
        self.assertEqual(r, {"ok": True})
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("user@test.com", "secret")
        self.assertEqual(mock_server.sendmail.call_count, 1)

    @patch("external_services.notify.smtplib.SMTP")
    def test_send_run_complete_uses_same_stack(self, _mock_smtp_class: MagicMock) -> None:
        mock_server = MagicMock()
        _mock_smtp_class.return_value.__enter__.return_value = mock_server
        with patch.dict(os.environ, self._smtp_env, clear=False):
            notify.send_run_complete_email(
                "run-uuid",
                "arkansas",
                10,
                75,
                total_contacts=100,
                total_with_emails=40,
                duration_seconds=120.0,
            )
        mock_server.sendmail.assert_called_once()

    def test_send_test_when_notifications_disabled(self) -> None:
        with patch.dict(os.environ, {"NOTIFY_ON_RUN_COMPLETE": "false"}, clear=False):
            r = notify.send_test_notification_email("School Scraper")
        self.assertFalse(r["ok"])
        self.assertIn("error", r)


if __name__ == "__main__":
    unittest.main()
