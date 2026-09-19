import signal
import threading
from .config import Settings
from .models import Delivery, Event
from .service import RelayService
from .store import Store


class DeliveryWorker:
    def __init__(self, service: RelayService, store: Store, settings: Settings,
                 poll_interval: float | None = None, batch_size: int | None = None):
        self.service = service
        self.store = store
        self.settings = settings
        self.poll_interval = settings.poll_interval_seconds if poll_interval is None else poll_interval
        self.batch_size = settings.worker_batch_size if batch_size is None else batch_size
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    def run_once(self) -> int:
        claimed = self.store.claim_due(self.batch_size)
        self._process(claimed)
        return len(claimed)

    def run(self) -> None:
        while not self._stop.is_set():
            if self.run_once() == 0:
                self._stop.wait(self.poll_interval)

    def _process(self, claimed: list[tuple[Event, Delivery]]) -> None:
        for index, (event, delivery) in enumerate(claimed):
            if self._stop.is_set():
                self.store.release_claimed([d.event_id for _, d in claimed[index:]])
                return
            self.service.deliver_once(event, delivery.attempts + 1)


def main():
    settings = Settings.from_env()
    store = Store(settings.database_path)
    worker = DeliveryWorker(RelayService(settings, store), store, settings)

    def _handle_signal(_signum, _frame):
        worker.stop()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    worker.run()


if __name__ == "__main__":
    main()
