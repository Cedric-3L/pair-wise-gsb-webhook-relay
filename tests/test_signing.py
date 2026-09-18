import unittest
from src.webhook_relay.signing import signature_for, verify_signature


class SigningTests(unittest.TestCase):
    def test_valid_signature(self):
        payload = b'{"id":1}'
        signature = signature_for(payload, "secret")
        self.assertTrue(verify_signature(payload, signature, "secret"))

    def test_tampered_payload_is_rejected(self):
        signature = signature_for(b"original", "secret")
        self.assertFalse(verify_signature(b"changed", signature, "secret"))
