from datetime import datetime, timedelta, timezone


def next_retry_at(attempt: int, base_seconds: float, now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    delay = base_seconds * (2 ** max(attempt - 1, 0))
    return (now + timedelta(seconds=delay)).isoformat()


def outcome(attempt: int, max_attempts: int, base_seconds: float, error: str | None = None, now: datetime | None = None) -> tuple[str, str | None]:
    if error is None:
        return "delivered", None
    if attempt >= max_attempts:
        return "dead_letter", None
    return "pending", next_retry_at(attempt, base_seconds, now)
