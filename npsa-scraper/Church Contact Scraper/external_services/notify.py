"""
Run-completion email notifications via Resend HTTP API (https://api.resend.com/emails).

Required: RESEND_API_KEY, NOTIFY_EMAIL
Optional: NOTIFY_FROM (default onboarding@resend.dev), NOTIFY_ON_RUN_COMPLETE=false to disable.
"""

from __future__ import annotations

import html
import os
from typing import Any, Optional

import requests

RESEND_API_URL = "https://api.resend.com/emails"
SCRAPER_SUBJECT_TAG = "Church Scraper"


def _is_enabled() -> bool:
    """True if Resend + recipient are configured and notifications are not explicitly disabled."""
    if os.getenv("NOTIFY_ON_RUN_COMPLETE", "true").lower() in ("false", "0", "no"):
        return False
    return bool(os.getenv("RESEND_API_KEY", "").strip() and os.getenv("NOTIFY_EMAIL", "").strip())


def _text_to_html(text: str) -> str:
    return (
        '<pre style="font-family:system-ui,sans-serif;white-space:pre-wrap">'
        f"{html.escape(text)}"
        "</pre>"
    )


def _send_resend_html(subject: str, html_body: str) -> None:
    """POST to Resend. Raises on configuration or API errors."""
    api_key = os.getenv("RESEND_API_KEY", "").strip()
    to_email = os.getenv("NOTIFY_EMAIL", "").strip()
    from_addr = os.getenv("NOTIFY_FROM", "onboarding@resend.dev").strip()
    if not api_key or not to_email:
        raise ValueError("Missing RESEND_API_KEY or NOTIFY_EMAIL")
    try:
        r = requests.post(
            RESEND_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": from_addr,
                "to": [to_email],
                "subject": subject,
                "html": html_body,
            },
            timeout=30,
        )
    except requests.RequestException as e:
        raise RuntimeError(f"Resend request failed: {e}") from e
    if not r.ok:
        raise RuntimeError(f"Resend API {r.status_code}: {r.text[:800]}")


def send_test_notification_email(service_label: str) -> dict[str, Any]:
    """
    Send a dummy email to NOTIFY_EMAIL to verify Resend without running a pipeline.

    Returns {"ok": True} or {"ok": False, "error": "..."}.
    """
    if not _is_enabled():
        return {
            "ok": False,
            "error": (
                "Email notifications disabled (NOTIFY_ON_RUN_COMPLETE=false) or "
                "missing RESEND_API_KEY / NOTIFY_EMAIL"
            ),
        }
    try:
        subject = f"[TEST] {service_label} — Resend check (NPSA)"
        text = (
            "This is an automated test message from the NPSA scraper email notifier.\n\n"
            "No state scrape was run — this is only a configuration check.\n\n"
            "If this arrived, your RESEND_API_KEY and NOTIFY_EMAIL settings are working.\n"
        )
        _send_resend_html(subject, _text_to_html(text))
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def send_run_complete_email(
    run_id: str,
    state: str,
    counties_processed: int,
    total_counties: int,
    total_contacts: int = 0,
    total_with_emails: int = 0,
    duration_seconds: Optional[float] = None,
) -> None:
    """
    Send a single run-completion notification. No-op if not configured or send fails.
    Call only when transitioning a run to "completed"; use notify_sent at the call site.
    """
    if not _is_enabled():
        print(
            f"[NOTIFY] Email notifications disabled (Run ID: {run_id}, State: {state}) — "
            "check NOTIFY_ON_RUN_COMPLETE, RESEND_API_KEY, NOTIFY_EMAIL"
        )
        return
    try:
        lines = [
            f"Run completed: {state}",
            f"Run ID: {run_id}",
            f"Counties: {counties_processed}/{total_counties}",
            f"Total contacts: {total_contacts}",
            f"Contacts with emails: {total_with_emails}",
        ]
        if duration_seconds is not None and duration_seconds >= 0:
            mins = int(duration_seconds // 60)
            secs = int(duration_seconds % 60)
            lines.append(f"Duration: {mins}m {secs}s")
        body_text = "\n".join(lines)
        subject = f"[{SCRAPER_SUBJECT_TAG}] Run complete: {state}"
        _send_resend_html(subject, _text_to_html(body_text))
        print(
            f"[NOTIFY] Run completion email sent: {state} (Run ID: {run_id}, "
            f"Counties: {counties_processed}/{total_counties}, Contacts: {total_contacts})"
        )
    except Exception as e:
        print(f"[NOTIFY] Run completion email failed: {e} (Run ID: {run_id}, State: {state})")
