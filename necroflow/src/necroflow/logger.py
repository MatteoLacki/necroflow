from __future__ import annotations

import logging
import sys

_log = logging.getLogger("necroflow")


def setup() -> None:
    """Configure the necroflow logger if not already set up."""
    if _log.handlers:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", datefmt="%H:%M:%S"))
    _log.addHandler(handler)
    _log.setLevel(logging.INFO)


def job_start(node) -> None:
    cfg = ", ".join(f"{k}={v!r}" for k, v in node.config.items()) if node.config else ""
    desc = node.rule.__name__ if node.rule else "?"
    if cfg:
        desc += f" ({cfg})"
    threads = node.rule.constraints.get("threads", 1) if node.rule and node.rule.constraints else 1
    thread_str = f" [{threads} threads]" if threads > 1 else ""
    _log.info("start  %s → %s%s", desc, node.path, thread_str)


def job_done(node, elapsed: float) -> None:
    _log.info("done   %s in %.1fs", node.rule.__name__ if node.rule else "?", elapsed)


def job_failed(node, elapsed: float, returncode: int, log_path) -> None:
    label = "interrupted" if returncode < 0 else "failed"
    _log.error(
        "%s  %s in %.1fs (exit %d) — log: %s",
        label,
        node.rule.__name__ if node.rule else "?",
        elapsed,
        returncode,
        log_path,
    )


def job_error(node, elapsed: float, exc: Exception, log_path) -> None:
    _log.error(
        "error  %s in %.1fs (%s) — log: %s",
        node.rule.__name__ if node.rule else "?",
        elapsed,
        exc,
        log_path,
    )


def job_output(log_path) -> None:
    """Emit captured job output to the terminal after a failure."""
    try:
        content = log_path.read_text()
    except OSError:
        return
    if content.strip():
        _log.error("output:\n%s", content.rstrip())


def summary(n_run: int, n_skipped: int, n_failed: int) -> None:
    parts = []
    if n_run:
        parts.append(f"{n_run} completed")
    if n_skipped:
        parts.append(f"{n_skipped} skipped (up-to-date)")
    if n_failed:
        parts.append(f"{n_failed} failed")
    if not parts:
        parts = ["nothing to do"]
    _log.info("done: %s", ", ".join(parts))
