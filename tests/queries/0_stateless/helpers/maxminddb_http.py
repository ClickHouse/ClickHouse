#!/usr/bin/env python3
"""HTTP/HTTPS fixture origin with observable metadata requests and downloads."""

import hashlib
import json
import pathlib
import ssl
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

directory = pathlib.Path(sys.argv[1])
lock = threading.Lock()
counts = {"HEAD": 0, "GET": 0, "failed": 0}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def respond(self, head):
        state = (directory / "state").read_text().strip()
        url = urlsplit(self.path)
        parameters = parse_qs(url.query)
        auth = self.headers.get("Authorization")
        authorized = url.path != "/auth.mmdb" or auth == "Basic dGVzdDp0ZXN0"
        authorized &= url.path != "/token.mmdb" or auth == "Bearer fixture-token"
        if url.path == "/archive":
            authorized &= parameters == {
                "license_key": ["fixture-api-secret"],
                "suffix": ["tar.gz"],
                "opaque": ["{a,b}|c"],
            }
        if url.path == "/permalink":
            authorized &= auth == "Basic dGVzdDp0ZXN0"
            authorized &= parameters == {"suffix": ["tar.gz"]}
        if url.path == "/signed-download":
            authorized &= auth is None
            authorized &= parameters == {"X-Amz-Signature": ["fixture-signed-secret"]}
        with lock:
            counts["HEAD" if head else "GET"] += 1
            if state == "unavailable" or not authorized:
                counts["failed"] += 1
            next_stats = directory / "stats.next"
            next_stats.write_text(json.dumps(counts))
            next_stats.replace(directory / "stats")
        if state == "unavailable" or not authorized:
            self.send_response(503 if authorized else 401)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if url.path == "/permalink":
            if head:
                self.send_response(200)
                date = "Thu, 01" if state == "A" else "Fri, 02"
                self.send_header("Last-Modified", f"{date} Jan 2026 00:00:00 GMT")
            else:
                self.send_response(302)
                port = https.server_address[1]
                self.send_header(
                    "Location",
                    f"https://127.0.0.1:{port}/signed-download?X-Amz-Signature=fixture-signed-secret",
                )
                self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if url.path == "/error":
            body = self.path.encode()
            self.send_response(403)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if not head:
                self.wfile.write(body)
            return
        if url.path in ("/archive", "/signed-download"):
            filename = f"{state}.tar.gz"
        else:
            filename = f"v4_{state}.mmdb" if state in ("A", "B") else f"{state}.mmdb"
        body = (directory / filename).read_bytes()
        etag = '"' + hashlib.sha256(body).hexdigest() + '"'
        size = len(body)
        start, end = 0, size - 1
        range_header = self.headers.get("Range", "")
        if not head and range_header.startswith("bytes="):
            left, right = range_header[6:].split("-", 1)
            start = int(left)
            end = min(size - 1, int(right)) if right else size - 1
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        else:
            self.send_response(200)
        self.send_header("etag", etag)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(end - start + 1))
        self.end_headers()
        if not head:
            self.wfile.write(body[start : end + 1])

    def do_HEAD(self):
        self.respond(True)

    def do_GET(self):
        self.respond(False)

    def log_message(self, *_args):
        pass


http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
https = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
tls.load_cert_chain(sys.argv[2], sys.argv[3])
https.socket = tls.wrap_socket(https.socket, server_side=True)
threading.Thread(target=https.serve_forever, daemon=True).start()
print(
    json.dumps({"http": http.server_address[1], "https": https.server_address[1]}),
    flush=True,
)
http.serve_forever()
