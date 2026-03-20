"""
Structured operational logging for Church Contact Scraper (stdout).
Replaces verbose print spam with a compact, consistent format.
"""

from __future__ import annotations

import math
from typing import Optional

W = 55  # total width for dashed lines (content + leading dashes)


def _pad_line(inner: str, fill: str = "─") -> str:
    inner = inner.strip()
    if len(inner) >= W:
        return inner[: W - 3] + "..."
    return inner + fill * (W - len(inner))


def log_startup(run_id: str, state: str, county_count: int, worker_count: int) -> None:
    print("═══════════════════════════════════════════════════════")
    print("  Church Scraper — Operational")
    print(f"  Run ID: {run_id}")
    st = state.replace("_", " ").title()
    print(f"  State: {st} | Counties: {county_count} | Workers: {worker_count}")
    print("═══════════════════════════════════════════════════════")


def log_county_header(county: str, index_1_based: int, total: int) -> None:
    label = f"{county} County ({index_1_based}/{total})"
    print(_pad_line(f"── {label} ", "─"))


def log_church_success(name: str, n_contacts: int) -> None:
    print(f"  ✓ {name} — {n_contacts} contacts")


def log_church_skip(name: str, reason: str) -> None:
    print(f"  · {name} — {reason}")


def log_warn(message: str) -> None:
    print(f"  ⚠ {message}")


def log_err(message: str) -> None:
    print(f"  ✗ {message}")


def log_county_done(
    n_contacts: int,
    n_with_emails: int,
    minutes: float,
) -> None:
    body = f"  {n_contacts} contacts ({n_with_emails} with emails) · {minutes:.0f} min"
    print(_pad_line(f"──{body} ", "─"))


def log_progress_counties(completed: int, total: int, total_contacts: int) -> None:
    body = f" Progress: {completed}/{total} counties · {total_contacts} contacts"
    print(_pad_line(f"──{body} ", "─"))


def log_aggregation(
    total_scraped: int,
    with_emails: int,
    hunter_found: int,
    hunter_searched: int,
    final_count: int,
    csv_filename: str,
) -> None:
    print("═══════════════════════════════════════════════════════")
    print("  Aggregation")
    print(f"  Total scraped: {total_scraped:,} contacts ({with_emails:,} with emails)")
    pct = (100.0 * hunter_found / hunter_searched) if hunter_searched else 0.0
    print(
        f"  Hunter enrichment: {hunter_found:,} found / {hunter_searched:,} searched ({pct:.1f}%)"
    )
    print(f"  Final output: {final_count:,} contacts → {csv_filename}")
    print("═══════════════════════════════════════════════════════")


def log_cost_estimate(
    places_calls: int,
    hunter_credits: int,
    openai_calls: int,
    openai_input_tokens: int,
    openai_output_tokens: int,
    elapsed_hours: float,
    total_contacts: int,
) -> None:
    # Google Places Text Search Enterprise: $35/1k after 1k free / month (approximate)
    billable_places = max(0, places_calls - 1000)
    places_cost = billable_places * (35.0 / 1000.0)

    # Hunter: $50/1k credits on successful finds
    hunter_cost = hunter_credits * (50.0 / 1000.0)

    # GPT-4o-mini
    openai_cost = (openai_input_tokens / 1_000_000.0) * 0.15 + (
        openai_output_tokens / 1_000_000.0
    ) * 0.60

    railway_cost = elapsed_hours * 60.0 * 0.000463

    total_cost = places_cost + hunter_cost + openai_cost + railway_cost
    per = (total_cost / total_contacts) if total_contacts else 0.0

    print("  Cost Estimate")
    print(
        f"  Google Places: {places_calls:,} calls ({billable_places:,} billable) · ~${places_cost:.2f}"
    )
    print(f"  Hunter.io: {hunter_credits:,} credits · ~${hunter_cost:.2f}")
    print(
        f"  OpenAI: {openai_calls:,} calls · ~${openai_cost:.2f}"
    )
    print(f"  Railway: {elapsed_hours:.1f} hrs · ~${railway_cost:.2f}")
    print(f"  Total: ~${total_cost:.2f} · ${per:.3f}/contact")
    print("═══════════════════════════════════════════════════════")


def log_state_complete(state: str, completed: int, total: int, elapsed_hours: float) -> None:
    st = state.replace("_", " ").title()
    print(f"  {st} complete — {completed}/{total} counties · {elapsed_hours:.1f} hrs")
    print("═══════════════════════════════════════════════════════")
