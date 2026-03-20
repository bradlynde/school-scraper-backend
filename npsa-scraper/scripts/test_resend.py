#!/usr/bin/env python3
"""
Send a one-off test email through Resend — same HTTPS API as production notify.py.

Do NOT put your API key in this file. Set env vars first:

  export RESEND_API_KEY="re_xxxxxxxxx"   # replace with your real key from Resend dashboard
  export NOTIFY_EMAIL="you@example.com"   # recipient (Resend test mode may only allow your signup email)
  # optional:
  export NOTIFY_FROM="onboarding@resend.dev"

Then from repo root:
  python3 npsa-scraper/scripts/test_resend.py

Or from Church Contact Scraper (if cwd matters for your shell):
  cd "npsa-scraper/Church Contact Scraper" && python3 ../../scripts/test_resend.py
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

RESEND_API_URL = "https://api.resend.com/emails"


def main() -> int:
    api_key = os.getenv("RESEND_API_KEY", "").strip()
    to_email = os.getenv("NOTIFY_EMAIL", "").strip()
    from_addr = os.getenv("NOTIFY_FROM", "onboarding@resend.dev").strip()

    if not api_key or api_key == "re_xxxxxxxxx":
        print(
            "Set RESEND_API_KEY to your real key (not the placeholder).\n"
            "Example: export RESEND_API_KEY='re_...'",
            file=sys.stderr,
        )
        return 1
    if not to_email:
        print("Set NOTIFY_EMAIL to the recipient address.", file=sys.stderr)
        return 1

    payload = {
        "from": from_addr,
        "to": [to_email],
        "subject": "Hello World (NPSA Resend test)",
        "html": "<p>Congrats on sending your <strong>first email</strong> via Resend from the NPSA scraper test script.</p>",
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        RESEND_API_URL,
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        print("OK:", raw or "(empty body)")
        return 0
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace") if e.fp else ""
        print(f"HTTP {e.code}: {detail or e.reason}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
