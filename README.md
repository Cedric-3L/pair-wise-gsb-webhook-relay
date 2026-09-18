# Webhook Relay

A dependency-free Python service for receiving webhook events, validating HMAC signatures, persisting delivery attempts, and retrying failed downstream deliveries with exponential backoff.

## Scope

- Receive JSON events over HTTP.
- Validate `X-Webhook-Signature` using HMAC-SHA256.
- Persist events and delivery attempts in SQLite.
- Retry transient downstream failures with bounded exponential backoff.
- Move exhausted deliveries to a dead-letter state.

## Run

```bash
python -m unittest discover -s tests -v
PYTHONPATH=src python -m webhook_relay.http_server
```

The server listens on `127.0.0.1:8080` by default. Set `WEBHOOK_SECRET`, `RELAY_TARGET`, and `WEBHOOK_DB` to configure it.

## Endpoints

- `POST /webhooks` accepts a JSON object and requires `X-Webhook-Signature`.
- `GET /healthz` returns service health.

This repository is intentionally small but has multiple interacting modules so it can be used as a realistic coding-agent evaluation baseline.
