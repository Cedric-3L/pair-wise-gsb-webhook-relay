import json
import urllib.error
import urllib.request
from .config import Settings
from .models import Event, utc_now
from .retry import outcome
from .signing import verify_signature
from .store import Store


class RelayService:
    def __init__(self, settings: Settings, store: Store):
        self.settings = settings
        self.store = store

    def accept(self, raw_payload: bytes, signature: str | None, event_id: str) -> str:
        if not verify_signature(raw_payload, signature, self.settings.secret):
            raise ValueError("invalid webhook signature")
        json.loads(raw_payload)
        event = Event(event_id, raw_payload.decode("utf-8"), utc_now())
        return "accepted" if self.store.save_event(event) else "duplicate"

    def deliver_once(self, event: Event, attempts: int) -> str:
        request = urllib.request.Request(self.settings.target_url, data=event.payload.encode(), method="POST",
                                         headers={"Content-Type": "application/json"})
        error = None
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                if response.status >= 400:
                    error = f"downstream status {response.status}"
        except (urllib.error.URLError, TimeoutError) as exc:
            error = str(exc)
        status, next_at = outcome(attempts, self.settings.max_attempts, self.settings.base_backoff_seconds, error)
        self.store.mark_attempt(event.event_id, status, attempts, next_at, error)
        return status
