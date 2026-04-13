import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat()


@dataclass
class OutboxEvent:
    id: int
    channel_id: int
    event: str
    category: str
    scope_key: str
    idempotency_key: str
    callback_url: str
    webhook_secret: str
    payload: Dict[str, Any]
    delivery_attempts: int
    next_attempt_at: str
    created_at: str
    updated_at: str
    last_error: Optional[str]


class GatewayStore:
    def __init__(self, db_path: Path):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._configure()
        self._migrate()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def load_runtime_state(self, channel_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT runtime_state_json FROM channel_runtime_states WHERE channel_id = ?",
                (channel_id,),
            ).fetchone()

        if row is None or not row["runtime_state_json"]:
            return {}

        try:
            return json.loads(row["runtime_state_json"])
        except json.JSONDecodeError:
            return {}

    def save_runtime_state(self, channel_id: int, runtime_state: Dict[str, Any]) -> None:
        now = _iso_now()
        payload = json.dumps(runtime_state or {}, separators=(",", ":"), sort_keys=True)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO channel_runtime_states (
                    channel_id,
                    runtime_state_json,
                    updated_at
                ) VALUES (?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    runtime_state_json = excluded.runtime_state_json,
                    updated_at = excluded.updated_at
                """,
                (channel_id, payload, now),
            )
            self._conn.commit()

    def enqueue_event(
        self,
        *,
        channel_id: int,
        event: str,
        category: str,
        scope_key: str,
        idempotency_key: str,
        callback_url: str,
        webhook_secret: str,
        payload: Dict[str, Any],
    ) -> bool:
        now = _iso_now()
        payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO outbox_events (
                    channel_id,
                    event,
                    category,
                    scope_key,
                    idempotency_key,
                    callback_url,
                    webhook_secret,
                    payload_json,
                    status,
                    delivery_attempts,
                    next_attempt_at,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                """,
                (
                    channel_id,
                    event,
                    category,
                    scope_key,
                    idempotency_key,
                    callback_url,
                    webhook_secret,
                    payload_json,
                    now,
                    now,
                    now,
                ),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def due_events(self, *, channel_id: int, limit: int) -> List[OutboxEvent]:
        now = _iso_now()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT *
                FROM outbox_events
                WHERE channel_id = ?
                  AND status = 'pending'
                  AND next_attempt_at <= ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (channel_id, now, limit),
            ).fetchall()

        return [self._row_to_event(row) for row in rows]

    def mark_acked(self, event_id: int) -> None:
        now = _iso_now()
        with self._lock:
            self._conn.execute(
                """
                UPDATE outbox_events
                SET status = 'acked',
                    acked_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, now, event_id),
            )
            self._conn.commit()

    def mark_retry(
        self,
        event_id: int,
        *,
        delivery_attempts: int,
        next_attempt_at: str,
        error_message: str,
    ) -> None:
        now = _iso_now()
        with self._lock:
            self._conn.execute(
                """
                UPDATE outbox_events
                SET delivery_attempts = ?,
                    next_attempt_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (delivery_attempts, next_attempt_at, error_message, now, event_id),
            )
            self._conn.commit()

    def mark_dead(self, event_id: int, *, delivery_attempts: int, error_message: str) -> None:
        now = _iso_now()
        with self._lock:
            self._conn.execute(
                """
                UPDATE outbox_events
                SET status = 'dead_letter',
                    delivery_attempts = ?,
                    last_error = ?,
                    dead_lettered_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (delivery_attempts, error_message, now, now, event_id),
            )
            self._conn.commit()

    def count_events(
        self,
        *,
        channel_id: int,
        category: str,
        scope_key: str,
        status: str,
    ) -> int:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM outbox_events
                WHERE channel_id = ?
                  AND category = ?
                  AND scope_key = ?
                  AND status = ?
                """,
                (channel_id, category, scope_key, status),
            ).fetchone()
        return int(row["count"] if row else 0)

    def prune_finished_events(self, *, older_than_hours: int) -> int:
        threshold = (_utc_now() - timedelta(hours=max(older_than_hours, 1))).isoformat()
        with self._lock:
            cursor = self._conn.execute(
                """
                DELETE FROM outbox_events
                WHERE status IN ('acked', 'dead_letter')
                  AND updated_at < ?
                """,
                (threshold,),
            )
            self._conn.commit()
            return cursor.rowcount

    def requeue_dead_events(self, *, channel_id: int, category: str, scope_key: str) -> int:
        now = _iso_now()
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE outbox_events
                SET status = 'pending',
                    next_attempt_at = ?,
                    updated_at = ?
                WHERE channel_id = ?
                  AND category = ?
                  AND scope_key = ?
                  AND status = 'dead_letter'
                """,
                (now, now, channel_id, category, scope_key),
            )
            self._conn.commit()
            return cursor.rowcount

    def _configure(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.commit()

    def _migrate(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS outbox_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id INTEGER NOT NULL,
                    event TEXT NOT NULL,
                    category TEXT NOT NULL,
                    scope_key TEXT NOT NULL DEFAULT '',
                    idempotency_key TEXT NOT NULL,
                    callback_url TEXT NOT NULL,
                    webhook_secret TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    delivery_attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT NOT NULL,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    acked_at TEXT,
                    dead_lettered_at TEXT,
                    UNIQUE(channel_id, event, scope_key, idempotency_key)
                );

                CREATE INDEX IF NOT EXISTS idx_outbox_events_due
                    ON outbox_events (channel_id, status, next_attempt_at, id);

                CREATE TABLE IF NOT EXISTS channel_runtime_states (
                    channel_id INTEGER PRIMARY KEY,
                    runtime_state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self._conn.commit()

    def _row_to_event(self, row: sqlite3.Row) -> OutboxEvent:
        return OutboxEvent(
            id=int(row["id"]),
            channel_id=int(row["channel_id"]),
            event=str(row["event"]),
            category=str(row["category"]),
            scope_key=str(row["scope_key"] or ""),
            idempotency_key=str(row["idempotency_key"]),
            callback_url=str(row["callback_url"]),
            webhook_secret=str(row["webhook_secret"]),
            payload=json.loads(row["payload_json"]),
            delivery_attempts=int(row["delivery_attempts"] or 0),
            next_attempt_at=str(row["next_attempt_at"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            last_error=row["last_error"],
        )
