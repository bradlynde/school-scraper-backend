"""
Structured operational logging for Church Contact Scraper (stdout).
Compact format; ASCII-only symbols so Railway / log viewers don't mojibake UTF-8.
Set CHURCH_LOG_UNICODE=1 for Unicode symbols and box chars.

Pool workers send lines through a multiprocessing.Queue so the main process prints
one line at a time (no interleaving). Main process prints directly with flush=True
so the startup banner appears before worker output when stdout is piped (Railway).
"""

from __future__ import annotations

import os
import threading
from typing import Any, Optional

W = 55  # total width for dashed lines (content + trailing fill)

# Set in Pool worker processes via configure_worker_log_queue (main stays None).
_worker_log_queue: Optional[Any] = None

# Serialize stdout from the drain thread and the main thread (progress lines, etc.).
_print_lock = threading.Lock()


def configure_worker_log_queue(q: Any) -> None:
    """Pool initializer: send all structured log lines through this queue."""
    global _worker_log_queue
    _worker_log_queue = q


def clear_worker_log_queue() -> None:
    """Reset after pool shutdown (main process)."""
    global _worker_log_queue
    _worker_log_queue = None


def start_stdout_drain_thread(q: Any) -> threading.Thread:
    """
    Main process: one thread reads log lines from workers and prints with flush.
    Stop by putting None on the queue after all workers exit.
    """

    def _drain() -> None:
        while True:
            line = q.get()
            if line is None:
                break
            with _print_lock:
                print(line, flush=True)

    t = threading.Thread(target=_drain, name="church-scraper-log-drain", daemon=True)
    t.start()
    return t


def _emit(line: str) -> None:
    wq = _worker_log_queue
    if wq is not None:
        wq.put(line)
    else:
        with _print_lock:
            print(line, flush=True)


def _use_unicode() -> bool:
    return os.environ.get("CHURCH_LOG_UNICODE", "").strip() in ("1", "true", "yes")


def _sym(ok: str, ascii_alt: str) -> str:
    return ok if _use_unicode() else ascii_alt


def _pad_line(inner: str, fill: str = "-") -> str:
    inner = inner.strip()
    if len(inner) >= W:
        return inner[: W - 3] + "..."
    return inner + fill * (W - len(inner))


def log_startup(run_id: str, state: str, county_count: int, worker_count: int) -> None:
    bar = "═" * 55 if _use_unicode() else "=" * 55
    _emit(bar)
    _emit("  Church Scraper - Operational")
    _emit(f"  Run ID: {run_id}")
    st = state.replace("_", " ").title()
    _emit(f"  State: {st} | Counties: {county_count} | Workers: {worker_count}")
    _emit(bar)


def log_county_header(county: str, index_1_based: int, total: int) -> None:
    label = f"{county} County ({index_1_based}/{total})"
    dash = "─" if _use_unicode() else "-"
    _emit(_pad_line(f"-- {label} ", dash))


def log_church_success(name: str, n_contacts: int) -> None:
    mark = _sym("\u2713", "+")
    sep = " - " if not _use_unicode() else " \u2014 "
    _emit(f"  {mark} {name}{sep}{n_contacts} contacts")


def log_church_skip(name: str, reason: str) -> None:
    mark = _sym("\u00b7", ".")
    sep = " - " if not _use_unicode() else " \u2014 "
    _emit(f"  {mark} {name}{sep}{reason}")


def log_warn(message: str) -> None:
    mark = _sym("\u26a0", "!")
    _emit(f"  {mark} {message}")


def log_err(message: str) -> None:
    mark = _sym("\u2717", "X")
    _emit(f"  {mark} {message}")


def log_county_done(
    n_contacts: int,
    n_with_emails: int,
    minutes: float,
) -> None:
    dash = "─" if _use_unicode() else "-"
    body = f"  {n_contacts} contacts ({n_with_emails} with emails) - {minutes:.0f} min"
    _emit(_pad_line(f"--{body} ", dash))


def log_progress_counties(completed: int, total: int, total_contacts: int) -> None:
    dash = "─" if _use_unicode() else "-"
    body = f" Progress: {completed}/{total} counties - {total_contacts} contacts"
    _emit(_pad_line(f"--{body} ", dash))


def log_aggregation(
    total_scraped: int,
    with_emails: int,
    hunter_found: int,
    hunter_searched: int,
    final_count: int,
    csv_filename: str,
) -> None:
    bar = "═" * 55 if _use_unicode() else "=" * 55
    _emit(bar)
    _emit("  Aggregation")
    _emit(f"  Total scraped: {total_scraped:,} contacts ({with_emails:,} with emails)")
    pct = (100.0 * hunter_found / hunter_searched) if hunter_searched else 0.0
    _emit(
        f"  Hunter enrichment: {hunter_found:,} found / {hunter_searched:,} searched ({pct:.1f}%)"
    )
    _emit(f"  Final output: {final_count:,} contacts -> {csv_filename}")
    _emit(bar)


def log_cost_estimate(
    places_calls: int,
    hunter_credits: int,
    openai_calls: int,
    openai_input_tokens: int,
    openai_output_tokens: int,
    elapsed_hours: float,
    total_contacts: int,
) -> None:
    billable_places = max(0, places_calls - 1000)
    places_cost = billable_places * (35.0 / 1000.0)
    hunter_cost = hunter_credits * (50.0 / 1000.0)
    openai_cost = (openai_input_tokens / 1_000_000.0) * 0.15 + (
        openai_output_tokens / 1_000_000.0
    ) * 0.60
    railway_cost = elapsed_hours * 60.0 * 0.000463
    total_cost = places_cost + hunter_cost + openai_cost + railway_cost
    per = (total_cost / total_contacts) if total_contacts else 0.0
    bar = "═" * 55 if _use_unicode() else "=" * 55

    _emit("  Cost Estimate")
    _emit(
        f"  Google Places: {places_calls:,} calls ({billable_places:,} billable) - ~${places_cost:.2f}"
    )
    _emit(f"  Hunter.io: {hunter_credits:,} credits - ~${hunter_cost:.2f}")
    _emit(f"  OpenAI: {openai_calls:,} calls - ~${openai_cost:.2f}")
    _emit(f"  Railway: {elapsed_hours:.1f} hrs - ~${railway_cost:.2f}")
    _emit(f"  Total: ~${total_cost:.2f} - ${per:.3f}/contact")
    _emit(bar)


def log_state_complete(state: str, completed: int, total: int, elapsed_hours: float) -> None:
    st = state.replace("_", " ").title()
    bar = "═" * 55 if _use_unicode() else "=" * 55
    _emit(f"  {st} complete - {completed}/{total} counties - {elapsed_hours:.1f} hrs")
    _emit(bar)
