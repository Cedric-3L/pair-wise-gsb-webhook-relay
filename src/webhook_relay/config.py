from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    secret: str = "development-secret"
    target_url: str = "http://127.0.0.1:9090/events"
    database_path: str = "webhook-relay.db"
    max_attempts: int = 4
    base_backoff_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            secret=os.getenv("WEBHOOK_SECRET", cls.secret),
            target_url=os.getenv("RELAY_TARGET", cls.target_url),
            database_path=os.getenv("WEBHOOK_DB", cls.database_path),
            max_attempts=int(os.getenv("MAX_ATTEMPTS", cls.max_attempts)),
            base_backoff_seconds=float(os.getenv("BASE_BACKOFF_SECONDS", cls.base_backoff_seconds)),
        )
