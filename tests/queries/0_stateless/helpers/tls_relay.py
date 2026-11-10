#!/usr/bin/env python3
"""A single-connection TCP relay that can terminate TLS on either side.

Usage: tls_relay.py <front: raw|tls> <backend host> <backend port> <back: raw|tls> <cert file> <key file> <port file>

The relay accepts one connection and forwards the bytes in both directions, then exits.
- front `tls`: the relay is the TLS server for the client (with the given certificate and key);
  front `raw`: the relay forwards the client's bytes as they are.
- back `tls`: the relay opens its own TLS session to the backend (without verifying it);
  back `raw`: the relay connects to the backend over plain TCP.

With `raw` on both sides it is a passthrough: the client and the server share one TLS session.
With `tls` on the front the client and the server are on different sessions (or the server is on plaintext).

The port the relay listens on is chosen by the OS and written to the port file atomically.
"""

import os
import select
import socket
import ssl
import sys
import time

front, backend_host, backend_port, back, cert_file, key_file, port_file = sys.argv[1:]
backend_port = int(backend_port)

listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", 0))
listener.listen(1)
# Never hang the test if the client does not come.
listener.settimeout(60)

with open(port_file + ".tmp", "w") as f:
    f.write(str(listener.getsockname()[1]))
os.rename(port_file + ".tmp", port_file)

client, _ = listener.accept()
listener.close()
client.settimeout(60)

if front == "tls":
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(cert_file, key_file)
    client = server_context.wrap_socket(client, server_side=True)

backend = socket.create_connection((backend_host, backend_port), timeout=60)
if back == "tls":
    client_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    client_context.check_hostname = False
    client_context.verify_mode = ssl.CERT_NONE
    backend = client_context.wrap_socket(backend, server_hostname=backend_host)


# Both directions are forwarded by this one thread. An `ssl.SSLSocket` must not be read and written
# from two threads at the same time: the underlying OpenSSL object is not thread-safe, and doing so
# loses data or crashes. The sockets are non-blocking, because a TLS socket can be readable
# without having application data (e.g. a TLS 1.3 session ticket), and a blocking `recv` would then
# wait while the other direction has data to forward.
WANT = (ssl.SSLWantReadError, ssl.SSLWantWriteError, BlockingIOError)
deadline = time.monotonic() + 60
peer = {client: backend, backend: client}
for s in peer:
    s.setblocking(False)


def send_all(dst, data):
    while data:
        try:
            data = data[dst.send(data) :]
        except WANT:
            select.select([dst], [dst], [], max(0, deadline - time.monotonic()))
            if time.monotonic() > deadline:
                raise TimeoutError("timed out sending")


def forward(src):
    """Forwards what `src` has. Returns False at the end of the stream."""
    while True:
        try:
            data = src.recv(65536)
        except WANT:
            return True
        except OSError:
            return False
        if not data:
            return False
        send_all(peer[src], data)
        # Decrypted bytes buffered inside an SSL socket are not visible to `select`, drain them now.
        if not (isinstance(src, ssl.SSLSocket) and src.pending()):
            return True


# One direction ending is enough: after an authentication failure the server closes the connection.
try:
    while time.monotonic() < deadline:
        readable, _, _ = select.select(list(peer), [], [], deadline - time.monotonic())
        if not all(forward(s) for s in readable):
            break
finally:
    for s in peer:
        try:
            s.close()
        except OSError:
            pass
