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
import socket
import ssl
import sys
import threading

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


def pump(src, dst, done):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    done.set()


done = threading.Event()
threading.Thread(target=pump, args=(client, backend, done), daemon=True).start()
threading.Thread(target=pump, args=(backend, client, done), daemon=True).start()
# One direction ending is enough: after an authentication failure the server closes the connection.
done.wait(60)
for s in (client, backend):
    try:
        s.close()
    except OSError:
        pass
