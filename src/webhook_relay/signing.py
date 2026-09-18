import hashlib
import hmac


def signature_for(payload: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify_signature(payload: bytes, provided: str | None, secret: str) -> bool:
    if not provided:
        return False
    expected = signature_for(payload, secret)
    return hmac.compare_digest(expected, provided.strip())
