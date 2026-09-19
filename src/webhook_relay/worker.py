"""Background delivery worker with atomic claiming and graceful shutdown."""
import argparse
import signal
import threading
import urllib.error
import urllib.request
from collections.abc import Callable

from .config import Settings
from .models import Delivery, Event
from .retry import outcome
from .store import Store


def post_event(target_url: str, event: Event, timeout: float = 5.0) -> str | None:
    """POST the event payload downstream. Returns None on success, else an error string."""
    request = urllib.request.Request(target_url, data=event.payload.encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if response.status >= 400:
                return f"downstream status {response.status}"
    except (urllib.error.URLError, TimeoutError) as exc:
        return str(exc)
    return None


class DeliveryWorker:
    def __init__(self, settings: Settings, store: Store,
                 sender: Callable[[Event], str | None] | None = None,
                 poll_interval: float = 1.0, batch_size: int = 10,
                 lease_seconds: float = 30.0):
        self.settings = settings
        self.store = store
        self._sender = sender or (lambda event: post_event(settings.target_url, event))
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        self._stop_event = threading.Event()

    def stop(self) -> None:
        """Request graceful shutdown; the in-flight delivery finishes first."""
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    def _attempt(self, event: Event, delivery: Delivery) -> str:
        attempt = delivery.attempts + 1
        error = self._sender(event)
        status, next_at = outcome(attempt, self.settings.max_attempts,
                                  self.settings.base_backoff_seconds, error)
        self.store.mark_attempt(event.event_id, status, attempt, next_at, error)
        return status

    def run_once(self) -> int:
        """Claim a batch of due deliveries and deliver them. Returns the batch size."""
        claimed = self.store.claim_due_deliveries(limit=self.batch_size,
                                                  lease_seconds=self.lease_seconds)
        for event, delivery in claimed:
            if self._stop_event.is_set():
                break
            self._attempt(event, delivery)
        return len(claimed)

    def run(self) -> None:
        while not self._stop_event.is_set():
            if self.run_once() == 0:
                self._stop_event.wait(self.poll_interval)


def run_workers(settings: Settings, num_workers: int = 1, poll_interval: float = 1.0) -> None:
    """Run delivery workers in the foreground until SIGINT/SIGTERM, then shut down gracefully."""
    store = Store(settings.database_path)
    workers = [DeliveryWorker(settings, store, poll_interval=poll_interval)
               for _ in range(num_workers)]
    threads = [threading.Thread(target=worker.run, name=f"delivery-worker-{i}")
               for i, worker in enumerate(workers)]

    def handle_signal(_signum, _frame):
        for worker in workers:
            worker.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def main() -> None:
    parser = argparse.ArgumentParser(description="Webhook relay delivery worker")
    parser.add_argument("--workers", type=int, default=1, help="number of concurrent worker threads")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="seconds between scans when idle")
    args = parser.parse_args()
    run_workers(Settings.from_env(), num_workers=args.workers, poll_interval=args.poll_interval)


if __name__ == "__main__":
    main()
