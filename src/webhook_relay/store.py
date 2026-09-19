import sqlite3
from contextlib import contextmanager
from .models import Delivery, Event, utc_now


class Store:
    def __init__(self, path: str):
        self.path = path
        self._initialize()

    @contextmanager
    def connection(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _initialize(self) -> None:
        with self.connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY, payload TEXT NOT NULL, received_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                    event_id TEXT PRIMARY KEY REFERENCES events(event_id),
                    status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT, last_error TEXT
                );
            """)

    def save_event(self, event: Event) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO events(event_id, payload, received_at) VALUES (?, ?, ?)",
                (event.event_id, event.payload, event.received_at),
            )
            if cursor.rowcount == 0:
                return False
            conn.execute("INSERT INTO deliveries(event_id, status) VALUES (?, 'pending')", (event.event_id,))
            return True

    def get_delivery(self, event_id: str) -> Delivery | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM deliveries WHERE event_id = ?", (event_id,)).fetchone()
        return Delivery(**dict(row)) if row else None

    def mark_attempt(self, event_id: str, status: str, attempts: int, next_attempt_at: str | None, error: str | None) -> None:
        with self.connection() as conn:
            conn.execute("UPDATE deliveries SET status=?, attempts=?, next_attempt_at=?, last_error=? WHERE event_id=?",
                         (status, attempts, next_attempt_at, error, event_id))

    def due_events(self, now: str | None = None) -> list[tuple[Event, Delivery]]:
        now = now or utc_now()
        with self.connection() as conn:
            rows = conn.execute("""
                SELECT e.*, d.status, d.attempts, d.next_attempt_at, d.last_error
                FROM events e JOIN deliveries d ON d.event_id=e.event_id
                WHERE d.status='pending' AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?)
                ORDER BY e.received_at
            """, (now,)).fetchall()
        return [(Event(row['event_id'], row['payload'], row['received_at']),
                 Delivery(row['event_id'], row['status'], row['attempts'], row['next_attempt_at'], row['last_error']))
                for row in rows]

    def claim_due(self, limit: int, now: str | None = None) -> list[tuple[Event, Delivery]]:
        now = now or utc_now()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute("""
                SELECT e.*, d.status, d.attempts, d.next_attempt_at, d.last_error
                FROM events e JOIN deliveries d ON d.event_id=e.event_id
                WHERE d.status='pending' AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?)
                ORDER BY e.received_at
                LIMIT ?
            """, (now, limit)).fetchall()
            conn.executemany(
                "UPDATE deliveries SET status='claimed' WHERE event_id=? AND status='pending'",
                [(row['event_id'],) for row in rows],
            )
        return [(Event(row['event_id'], row['payload'], row['received_at']),
                 Delivery(row['event_id'], row['status'], row['attempts'], row['next_attempt_at'], row['last_error']))
                for row in rows]

    def release_claimed(self, event_ids: list[str]) -> None:
        if not event_ids:
            return
        with self.connection() as conn:
            conn.executemany(
                "UPDATE deliveries SET status='pending' WHERE event_id=? AND status='claimed'",
                [(event_id,) for event_id in event_ids],
            )
