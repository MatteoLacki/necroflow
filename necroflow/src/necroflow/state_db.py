from __future__ import annotations

import sqlite3
from pathlib import Path

_COMPROMISED = {"running", "failed", "interrupted"}

_CREATE = """
CREATE TABLE IF NOT EXISTS node_runs (
    node_key TEXT PRIMARY KEY,
    state    TEXT NOT NULL
);
"""


class StateDB:
    """Persistent run state in outdir/.rip/state.db (SQLite).

    Tracks per-node state across invocations so interrupted or failed nodes
    are re-executed even when their output file still exists on disk.
    """

    def __init__(self, outdir: Path) -> None:
        db_dir = Path(outdir) / ".rip"
        db_dir.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            db_dir / "state.db",
            check_same_thread=False,
        )
        self._conn.execute(_CREATE)
        self._conn.commit()

    def compromised_keys(self) -> set[str]:
        """Node keys whose last recorded state was running, failed, or interrupted."""
        rows = self._conn.execute(
            "SELECT node_key FROM node_runs WHERE state IN ('running','failed','interrupted')"
        ).fetchall()
        return {r[0] for r in rows}

    def mark_running(self, node_key: str) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO node_runs (node_key, state) VALUES (?, 'running')",
            (node_key,),
        )
        self._conn.commit()

    def mark_done(self, node_key: str, state: str) -> None:
        """state: 'up_to_date' | 'failed' | 'interrupted'"""
        self._conn.execute(
            "INSERT OR REPLACE INTO node_runs (node_key, state) VALUES (?, ?)",
            (node_key, state),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
