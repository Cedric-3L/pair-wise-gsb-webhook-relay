import json
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from src.webhook_relay.config import Settings
from src.webhook_relay.models import Event, utc_now
from src.webhook_relay.service import RelayService
from src.webhook_relay.store import Store
from src.webhook_relay.worker import DeliveryWorker


class DownstreamServer:
    def __init__(self, status=200, delay=0.0):
        self.bodies = []
        self.lock = threading.Lock()
        self.started = threading.Event()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                outer.started.set()
                if delay:
                    time.sleep(delay)
                with outer.lock:
                    outer.bodies.append(body.decode())
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *_args):
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/events"

    def received(self):
        with self.lock:
            return list(self.bodies)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class WorkerTestCase(unittest.TestCase):
    downstream_status = 200
    downstream_delay = 0.0

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.downstream = DownstreamServer(self.downstream_status, self.downstream_delay)

    def tearDown(self):
        self.downstream.close()
        self.tmpdir.cleanup()

    def make_worker(self, max_attempts=4, base_backoff=60.0, **worker_kwargs):
        settings = Settings(
            target_url=self.downstream.url,
            database_path=f"{self.tmpdir.name}/test.db",
            max_attempts=max_attempts,
            base_backoff_seconds=base_backoff,
        )
        store = Store(settings.database_path)
        service = RelayService(settings, store)
        worker = DeliveryWorker(service, store, settings, poll_interval=0.02, **worker_kwargs)
        return worker, store

    def save_events(self, store, count, prefix="evt"):
        for i in range(count):
            event_id = f"{prefix}-{i}"
            store.save_event(Event(event_id, json.dumps({"id": event_id}), utc_now()))


class DeliveryPathTests(WorkerTestCase):
    def test_successful_delivery_is_marked_delivered(self):
        worker, store = self.make_worker()
        self.save_events(store, 1)
        self.assertEqual(worker.run_once(), 1)
        delivery = store.get_delivery("evt-0")
        self.assertEqual(delivery.status, "delivered")
        self.assertEqual(delivery.attempts, 1)
        self.assertIsNone(delivery.next_attempt_at)
        self.assertIsNone(delivery.last_error)
        self.assertEqual(self.downstream.received(), [json.dumps({"id": "evt-0"})])

    def test_transient_failure_is_rescheduled_with_backoff(self):
        self.downstream.close()
        self.downstream = DownstreamServer(status=500)
        worker, store = self.make_worker(max_attempts=4, base_backoff=60.0)
        self.save_events(store, 1)
        before = utc_now()
        worker.run_once()
        delivery = store.get_delivery("evt-0")
        self.assertEqual(delivery.status, "pending")
        self.assertEqual(delivery.attempts, 1)
        self.assertIsNotNone(delivery.last_error)
        self.assertGreater(delivery.next_attempt_at, before)
        self.assertEqual(store.claim_due(10), [])

    def test_exhausted_delivery_goes_to_dead_letter(self):
        self.downstream.close()
        self.downstream = DownstreamServer(status=500)
        worker, store = self.make_worker(max_attempts=1)
        self.save_events(store, 1)
        worker.run_once()
        delivery = store.get_delivery("evt-0")
        self.assertEqual(delivery.status, "dead_letter")
        self.assertEqual(delivery.attempts, 1)
        self.assertIsNone(delivery.next_attempt_at)
        self.assertEqual(store.claim_due(10), [])

    def test_run_once_returns_zero_when_nothing_due(self):
        worker, _store = self.make_worker()
        self.assertEqual(worker.run_once(), 0)


class ConcurrencyTests(WorkerTestCase):
    def test_concurrent_claims_are_disjoint(self):
        worker, store = self.make_worker()
        self.save_events(store, 30)
        claimed_by_thread = [[], []]

        def claim_all(index):
            while True:
                batch = store.claim_due(5)
                if not batch:
                    return
                claimed_by_thread[index].extend(d.event_id for _, d in batch)

        threads = [threading.Thread(target=claim_all, args=(i,)) for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        all_claimed = claimed_by_thread[0] + claimed_by_thread[1]
        self.assertEqual(len(all_claimed), 30)
        self.assertEqual(len(set(all_claimed)), 30)

    def test_concurrent_workers_deliver_exactly_once(self):
        worker_a, store = self.make_worker()
        worker_b, _ = self.make_worker()
        self.save_events(store, 20)
        threads = [threading.Thread(target=w.run) for w in (worker_a, worker_b)]
        for thread in threads:
            thread.start()
        deadline = time.time() + 10
        while time.time() < deadline and len(self.downstream.received()) < 20:
            time.sleep(0.02)
        worker_a.stop()
        worker_b.stop()
        for thread in threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())

        received = self.downstream.received()
        self.assertEqual(len(received), 20)
        self.assertEqual(len(set(received)), 20)
        for i in range(20):
            delivery = store.get_delivery(f"evt-{i}")
            self.assertEqual(delivery.status, "delivered")
            self.assertEqual(delivery.attempts, 1)


class GracefulShutdownTests(WorkerTestCase):
    def test_stop_waits_for_inflight_delivery(self):
        self.downstream.close()
        self.downstream = DownstreamServer(status=200, delay=0.3)
        worker, store = self.make_worker()
        self.save_events(store, 1)
        thread = threading.Thread(target=worker.run)
        thread.start()
        self.assertTrue(self.downstream.started.wait(timeout=5))
        worker.stop()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(store.get_delivery("evt-0").status, "delivered")
        self.assertEqual(len(self.downstream.received()), 1)

    def test_unprocessed_claimed_deliveries_are_released_on_stop(self):
        worker, store = self.make_worker()
        self.save_events(store, 3)
        claimed = store.claim_due(10)
        self.assertEqual(len(claimed), 3)
        worker.stop()
        worker._process(claimed)
        for i in range(3):
            self.assertEqual(store.get_delivery(f"evt-{i}").status, "pending")
        self.assertEqual(self.downstream.received(), [])
        self.assertEqual(len(store.claim_due(10)), 3)


if __name__ == "__main__":
    unittest.main()
