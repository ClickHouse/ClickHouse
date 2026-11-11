#!/usr/bin/env python3

# A remote server can reflect the request's bearer token in the reason phrase of its error response. The token must be
# scrubbed there as in the body: absent from the error and from system.query_log, while the rest of the reason stays.

import os
import socket
import sys
import threading
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT_HTTP = os.environ.get("CLICKHOUSE_PORT_HTTP", "8123")
CLICKHOUSE_URL = os.environ.get(
    "CLICKHOUSE_URL", f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT_HTTP}/"
)

TOKEN_HEAD = "reasontok"
TOKEN_TAIL = uuid.uuid4().hex
TOKEN = TOKEN_HEAD + TOKEN_TAIL


class ReflectingHandler(BaseHTTPRequestHandler):
    def reply(self, with_body):
        auth = self.headers.get("Authorization", "")
        token = auth[len("Bearer ") :] if auth.startswith("Bearer ") else ""
        body = b"access denied"
        self.send_response(401, "invalid token " + token)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if with_body:
            self.wfile.write(body)

    def do_HEAD(self):
        self.reply(False)

    def do_GET(self):
        self.reply(True)

    def log_message(self, format, *args):
        pass


class HTTPServerV6(HTTPServer):
    address_family = socket.AF_INET6


def query(sql, query_id=None):
    url = CLICKHOUSE_URL
    if query_id:
        url += ("&" if "?" in url else "?") + "query_id=" + query_id
    try:
        return urllib.request.urlopen(url, data=sql.encode()).read().decode()
    except urllib.error.HTTPError as e:
        return e.read().decode()


def main():
    host = socket.getaddrinfo(CLICKHOUSE_HOST, None)[0][4][0]
    is_ipv6 = ":" in host
    httpd = (HTTPServerV6 if is_ipv6 else HTTPServer)((host, 0), ReflectingHandler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    peer = f"[{host}]" if is_ipv6 else host
    peer_url = f"http://{peer}:{httpd.server_address[1]}/"

    query_id = str(uuid.uuid4())
    error = query(
        f"SELECT * FROM url('{peer_url}', 'CSV', 'x UInt8', "
        f"headers('Authorization' = 'Bearer {TOKEN}')) SETTINGS http_max_tries = 1",
        query_id,
    )
    if TOKEN in error:
        print("reason_scrubbed: FAIL cleartext")
    elif "'invalid token [HIDDEN]'" in error:
        print("reason_scrubbed: OK")
    else:
        print("reason_scrubbed: FAIL no reflected reason")
        print(error, file=sys.stderr)

    # The needle is split so that this checking query does not itself contain the token.
    query("SYSTEM FLUSH LOGS query_log")
    counts = query(
        "SELECT countIf(position(exception, concat('{}', '{}')) > 0), "
        "countIf(position(exception, 'invalid token [HIDDEN]') > 0) "
        "FROM system.query_log WHERE event_date >= yesterday() AND query_id = '{}' "
        "AND current_database = currentDatabase() "
        "AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')".format(
            TOKEN_HEAD, TOKEN_TAIL, query_id
        )
    ).strip()
    print("query_log_scrubbed: " + ("OK" if counts == "0\t1" else "FAIL " + counts))

    httpd.shutdown()


if __name__ == "__main__":
    main()
