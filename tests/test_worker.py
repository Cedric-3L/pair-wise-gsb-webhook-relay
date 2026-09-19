import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from src.webhook_relay.config import Settings
from src.webhook_relay.models import Event, utc_now
from src.webhook_relay.store import Store
from src.webhook_relay.worker import DeliveryWorker


class WorkerTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        db_path = str(Path(self.tmpdir.name) / "test.db")
        self.settings = Settings(secret="s", target_url="http://unused/", database_path=db_path,
                                 max_attempts=3, base_backoff_seconds=2.0)
        self.store = Store(db_path)

    def seed(self, event_id: str, attempts: int = 0) -> Event:
        event = Event(event_id, '{"id": "%s"}' % event_id, utc_now())
        self.assertTrue(self.store.save_event(event))
        if attempts:
            self.store.mark_attempt(event_id, "pending", attempts, None, "prior failure")
        return event

    def make_worker(self, sender, **kwargs):
        kwargs.setdefault("poll_interval", 0.05)
        return DeliveryWorker(self.settings, self.store, sender=sender, **kwargs)


class DeliveryPathTests(WorkerTestCase):
    def test_success_marks_delivered(self):
        self.seed("evt-ok")
        worker = self.make_worker(sender=lambda event: None)
        self.assertEqual(worker.run_once(), 1)
        delivery = self.store.get_delivery("evt-ok")
        self.assertEqual(delivery.status, "delivered")
        self.assertEqual(delivery.attempts, 1)
        self.assertIsNone(delivery.next_attempt_at)
        self.assertIsNone(delivery.last_error)

    def test_transient_failure_schedules_retry_with_backoff(self):
        self.seed("evt-flaky")
        before = datetime.now(timezone.utc)
        worker = self.make_worker(sender=lambda event: "connection refused")
        worker.run_once()
        delivery = self.store.get_delivery("evt-flaky")
        self.assertEqual(delivery.status, "pending")
        self.assertEqual(delivery.attempts, 1)
        self.assertEqual(delivery.last_error, "connection refused")
        next_at = datetime.fromisoformat(delivery.next_attempt_at)
        self.assertGreaterEqual(next_at, before.replace(microsecond=0))
        self.assertGreater(next_at, before)  # backoff pushes it into the future

    def test_exhausted_attempts_move_to_dead_letter(self):
        self.seed("evt-doomed", attempts=self.settings.max_attempts - 1)
        worker = self.make_worker(sender=lambda event: "still broken")
        worker.run_once()
        delivery = self.store.get_delivery("evt-doomed")
        self.assertEqual(delivery.status, "dead_letter")
        self.assertEqual(delivery.attempts, self.settings.max_attempts)
        self.assertIsNone(delivery.next_attempt_at)
        self.assertEqual(delivery.last_error, "still broken")

    def test_failed_delivery_is_retried_after_backoff_elapses(self):
        self.seed("evt-retry")
        calls = []
        worker = self.make_worker(sender=lambda event: calls.append(1) or "boom")
        worker.run_once()
        self.assertEqual(worker.run_once(), 0)  # not yet due
        self.store.mark_attempt("evt-retry", "pending", 1, None, "boom")  # force due
        self.assertEqual(worker.run_once(), 1)
        self.assertEqual(len(calls), 2)


class ClaimTests(WorkerTestCase):
    def test_claim_is_atomic_across_concurrent_workers(self):
        for i in range(20):
            self.seed(f"evt-{i}")
        barrier = threading.Barrier(8)
        claimed_ids = []
        lock = threading.Lock()

        def claim_loop():
            barrier.wait(timeout=10)
            while True:
                batch = self.store.claim_due_deliveries(limit=3, lease_seconds=60)
                if not batch:
                    return
                with lock:
                    claimed_ids.extend(event.event_id for event, _ in batch)

        threads = [threading.Thread(target=claim_loop) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(len(claimed_ids), 20)
        self.assertEqual(len(set(claimed_ids)), 20)

    def test_in_progress_delivery_is_not_reclaimed_before_lease_expires(self):
        self.seed("evt-leased")
        first = self.store.claim_due_deliveries(lease_seconds=60)
        self.assertEqual([e.event_id for e, _ in first], ["evt-leased"])
        self.assertEqual(self.store.claim_due_deliveries(lease_seconds=60), [])

    def test_expired_lease_can_be_reclaimed(self):
        self.seed("evt-stuck")
        self.store.claim_due_deliveries(lease_seconds=60)
        past = "2000-01-01T00:00:00+00:00"
        with self.store.connection() as conn:
            conn.execute("UPDATE deliveries SET next_attempt_at=? WHERE event_id='evt-stuck'", (past,))
        reclaimed = self.store.claim_due_deliveries(lease_seconds=60)
        self.assertEqual([e.event_id for e, _ in reclaimed], ["evt-stuck"])

    def test_two_workers_do_not_deliver_the_same_event(self):
        for i in range(10):
            self.seed(f"evt-shared-{i}")
        sent = []
        lock = threading.Lock()

        def sender(event):
            with lock:
                sent.append(event.event_id)
            return None

        workers = [self.make_worker(sender=sender, batch_size=2) for _ in range(4)]
        threads = [threading.Thread(target=worker.run) for worker in workers]
        for thread in threads:
            thread.start()
        deadline = time.monotonic() + 10
        while len(sent) < 10 and time.monotonic() < deadline:
            time.sleep(0.01)
        for worker in workers:
            worker.stop()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(sorted(sent), [f"evt-shared-{i}" for i in range(10)])


class GracefulShutdownTests(WorkerTestCase):
    def test_stop_waits_for_in_flight_delivery(self):
        self.seed("evt-slow")
        started = threading.Event()
        release = threading.Event()

        def slow_sender(event):
            started.set()
            release.wait(timeout=10)
            return None

        worker = self.make_worker(sender=slow_sender)
        thread = threading.Thread(target=worker.run)
        thread.start()
        self.assertTrue(started.wait(timeout=5))
        worker.stop()
        time.sleep(0.1)
        self.assertTrue(thread.is_alive())  # still finishing the current delivery
        release.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.store.get_delivery("evt-slow").status, "delivered")

    def test_idle_worker_stops_promptly(self):
        worker = self.make_worker(sender=lambda event: None)
        thread = threading.Thread(target=worker.run)
        thread.start()
        time.sleep(0.1)
        worker.stop()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
