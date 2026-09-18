import unittest
from datetime import datetime, timezone
from src.webhook_relay.retry import next_retry_at, outcome


class RetryTests(unittest.TestCase):
    def test_backoff_doubles(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(next_retry_at(3, 2, now), "2026-01-01T00:00:08+00:00")

    def test_exhausted_delivery_goes_to_dead_letter(self):
        self.assertEqual(outcome(4, 4, 1.0, "timeout"), ("dead_letter", None))
