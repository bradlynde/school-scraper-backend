"""
Lightweight run-completion email notifications via SMTP (stdlib only).
Set SMTP_* and NOTIFY_EMAIL to enable. Optional: NOTIFY_ON_RUN_COMPLETE=false to disable.
"""

from __future__ import annotations

import os
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Any, Optional


def _is_enabled() -> bool:
    """True if all required env vars are set and notifications are not explicitly disabled."""
    if os.getenv("NOTIFY_ON_RUN_COMPLETE", "true").lower() in ("false", "0", "no"):
        return False
    return bool(
        os.getenv("SMTP_HOST")
        and os.getenv("SMTP_PORT")
        and os.getenv("SMTP_USER")
        and os.getenv("SMTP_PASSWORD")
        and os.getenv("NOTIFY_EMAIL")
    )


def _send_plain_email(subject: str, body: str) -> None:
    """Send one plain-text email via SMTP. Raises on configuration or SMTP errors."""
    host = os.getenv("SMTP_HOST", "").strip()
    port_str = os.getenv("SMTP_PORT", "587").strip()
    port = int(port_str) if port_str.isdigit() else 587
    user = os.getenv("SMTP_USER", "").strip()
    password = os.getenv("SMTP_PASSWORD", "").strip()
    to_email = os.getenv("NOTIFY_EMAIL", "").strip()
    if not all([host, user, password, to_email]):
        raise ValueError(
            "Missing SMTP_HOST, SMTP_USER, SMTP_PASSWORD, or NOTIFY_EMAIL"
        )
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to_email
    msg.attach(MIMEText(body, "plain", "utf-8"))
    context = ssl.create_default_context()
    with smtplib.SMTP(host, port) as server:
        server.starttls(context=context)
        server.login(user, password)
        server.sendmail(user, [to_email], msg.as_string())


def send_test_notification_email(service_label: str) -> dict[str, Any]:
    """
    Send a dummy email to NOTIFY_EMAIL to verify SMTP without running a pipeline.

    Returns {"ok": True} or {"ok": False, "error": "..."}.
    """
    if not _is_enabled():
        return {
            "ok": False,
            "error": (
                "Email notifications disabled (NOTIFY_ON_RUN_COMPLETE=false) or "
                "missing SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_PASSWORD/NOTIFY_EMAIL"
            ),
        }
    try:
        subject = f"[TEST] {service_label} — SMTP configuration check (NPSA)"
        body = (
            "This is an automated test message from the NPSA scraper email notifier.\n\n"
            "No state scrape was run — this is only a configuration check.\n\n"
            "If this arrived, your SMTP_* and NOTIFY_EMAIL settings are working.\n"
        )
        _send_plain_email(subject, body)
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
    Send a single run-completion notification. No-op if SMTP/NOTIFY_EMAIL not set or send fails.
    Call only when transitioning a run to "completed"; use a notify_sent flag at the call site to send at most once per run.
    """
    if not _is_enabled():
        print(
            f"[NOTIFY] Email notifications disabled (Run ID: {run_id}, State: {state}) - check NOTIFY_ON_RUN_COMPLETE env var"
        )
        return
    try:
        host = os.getenv("SMTP_HOST", "").strip()
        port_str = os.getenv("SMTP_PORT", "587").strip()
        user = os.getenv("SMTP_USER", "").strip()
        password = os.getenv("SMTP_PASSWORD", "").strip()
        to_email = os.getenv("NOTIFY_EMAIL", "").strip()
        if not all([host, user, password, to_email]):
            print(
                f"[NOTIFY] Email notification skipped - missing required env vars (Run ID: {run_id}, State: {state})"
            )
            print(
                f"[NOTIFY] Required: SMTP_HOST={bool(host)}, SMTP_PORT={bool(port_str)}, SMTP_USER={bool(user)}, SMTP_PASSWORD={'***' if password else 'missing'}, NOTIFY_EMAIL={bool(to_email)}"
            )
            return
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
        body = "\n".join(lines)
        subject = f"[School Scraper] Run complete: {state}"
        _send_plain_email(subject, body)

        print(
            f"[NOTIFY] Run completion email sent successfully: {state} (Run ID: {run_id}, Counties: {counties_processed}/{total_counties}, Contacts: {total_contacts}, Duration: {duration_seconds}s)"
        )
    except Exception as e:
        print(
            f"[NOTIFY] Run completion email failed: {e} (Run ID: {run_id}, State: {state})"
        )
