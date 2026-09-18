from dataclasses import dataclass
from datetime import datetime, timezone


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Event:
    event_id: str
    payload: str
    received_at: str


@dataclass(frozen=True)
class Delivery:
    event_id: str
    status: str
    attempts: int
    next_attempt_at: str | None
    last_error: str | None
