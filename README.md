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

## Background delivery worker

```bash
PYTHONPATH=src python -m webhook_relay.worker
```

The worker continuously claims due pending deliveries (atomically, so multiple
workers can run concurrently without double-delivering), POSTs them to
`RELAY_TARGET`, and retries transient failures with exponential backoff until
`MAX_ATTEMPTS` is reached, after which deliveries move to `dead_letter`.
Tune polling with `WORKER_POLL_INTERVAL` and `WORKER_BATCH_SIZE`.
Send `SIGTERM`/`SIGINT` for a graceful shutdown: in-flight deliveries finish
and unprocessed claimed deliveries are released back to `pending`.

## Endpoints

- `POST /webhooks` accepts a JSON object and requires `X-Webhook-Signature`.
- `GET /healthz` returns service health.

This repository is intentionally small but has multiple interacting modules so it can be used as a realistic coding-agent evaluation baseline.
