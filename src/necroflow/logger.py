from __future__ import annotations

import logging

_log = logging.getLogger("necroflow")


def setup() -> None:
    """Configure the necroflow logger if not already set up."""
    if _log.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(message)s", datefmt="%H:%M:%S")
    )
    _log.addHandler(handler)
    _log.setLevel(logging.INFO)


def job_start(call) -> None:
    desc = call.rule.__name__
    if call.config:
        desc += " (" + ", ".join(f"{k}={v!r}" for k, v in call.config.items()) + ")"
    threads = call.resources["threads"]
    thread_str = f" [{threads} threads]" if threads > 1 else ""
    _log.info("start  %s → %s%s", desc, call.workdir, thread_str)


def job_retry(call, failed_attempt: int, max_attempts: int, returncode: int) -> None:
    label = "interrupted" if returncode < 0 else "failed"
    _log.warning(
        "retry  %s after %s attempt %d/%d (exit %d)",
        call.rule.__name__,
        label,
        failed_attempt,
        max_attempts,
        returncode,
    )


def job_done(call, elapsed: float) -> None:
    _log.info("done   %s in %.1fs", call.rule.__name__, elapsed)


def job_failed(call, elapsed: float, exit_code: int | None, error: Exception) -> None:
    """Report a failed call, then echo its captured job output."""
    log_path = call.log_path()
    if exit_code is None:
        _log.error(
            "error  %s in %.1fs (%s) — log: %s",
            call.rule.__name__,
            elapsed,
            error,
            log_path,
        )
    else:
        _log.error(
            "%s  %s in %.1fs (exit %d) — log: %s",
            "interrupted" if exit_code < 0 else "failed",
            call.rule.__name__,
            elapsed,
            exit_code,
            log_path,
        )
    try:
        content = log_path.read_text()
    except OSError:
        return
    if content.strip():
        _log.error("output:\n%s", content.rstrip())


def cleaned(call) -> None:
    _log.info("clean  %s", call.workdir)


def unscheduled(calls) -> None:
    """Warn that the scheduler stopped while calls were still ready."""
    _log.warning(
        "stop   scheduler selected none of %d ready call(s): %s",
        len(calls),
        ", ".join(call.relative_path.as_posix() for call in calls),
    )


def dry_run_call(call, verdict: str) -> None:
    _log.info("would-run  %-8s  %s → %s", verdict, call.rule.__name__, call.workdir)


def dry_run_summary(n_would_run: int, n_up_to_date: int) -> None:
    parts = [f"{n_would_run} would run"]
    if n_up_to_date:
        parts.append(f"{n_up_to_date} up-to-date")
    _log.info("dry-run: %s", ", ".join(parts))


def summary(
    n_run: int, n_skipped: int, n_failed: int, n_cleaned: int = 0, n_blocked: int = 0
) -> None:
    parts = []
    if n_run:
        parts.append(f"{n_run} completed")
    if n_skipped:
        parts.append(f"{n_skipped} skipped (up-to-date)")
    if n_failed:
        parts.append(f"{n_failed} failed")
    if n_blocked:
        parts.append(f"{n_blocked} blocked")
    if n_cleaned:
        parts.append(f"{n_cleaned} cleaned")
    if not parts:
        parts = ["nothing to do"]
    _log.info("done: %s", ", ".join(parts))
