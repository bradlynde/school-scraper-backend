#!/usr/bin/env python3
"""
Send a real dummy email using the same SMTP code path as production notify.py.

Requires the same env vars as the Railway service:
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, NOTIFY_EMAIL
  NOTIFY_ON_RUN_COMPLETE=true (default)

Usage (from repo root):
  export SMTP_HOST=... SMTP_PORT=587 SMTP_USER=... SMTP_PASSWORD=... NOTIFY_EMAIL=...
  python3 npsa-scraper/scripts/test_smtp_notify.py --church
  python3 npsa-scraper/scripts/test_smtp_notify.py --school
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Send NPSA SMTP test email (no scrape).")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--church", action="store_true", help="Use Church Contact Scraper notify module")
    g.add_argument("--school", action="store_true", help="Use School Contact Scraper notify module")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    if args.church:
        scraper_root = root / "Church Contact Scraper"
        label = "Church Scraper"
    else:
        scraper_root = root / "School Contact Scraper"
        label = "School Scraper"

    sys.path.insert(0, str(scraper_root))
    from external_services import notify  # noqa: E402

    result = notify.send_test_notification_email(label)
    print(result)
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
