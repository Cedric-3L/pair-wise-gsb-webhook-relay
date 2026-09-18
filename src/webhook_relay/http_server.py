import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from .config import Settings
from .service import RelayService
from .store import Store


def make_handler(service: RelayService):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, body: dict):
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/healthz":
                self._send(200, {"status": "ok"})
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self):
            if self.path != "/webhooks":
                self._send(404, {"error": "not found"})
                return
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            try:
                result = service.accept(raw, self.headers.get("X-Webhook-Signature"), self.headers.get("X-Event-ID", "unknown"))
                self._send(202, {"status": result})
            except (ValueError, json.JSONDecodeError) as exc:
                self._send(400, {"error": str(exc)})

        def log_message(self, *_args):
            return

    return Handler


def main():
    settings = Settings.from_env()
    service = RelayService(settings, Store(settings.database_path))
    ThreadingHTTPServer(("127.0.0.1", 8080), make_handler(service)).serve_forever()


if __name__ == "__main__":
    main()
