#!/usr/bin/env python3
"""A TCP proxy that delays everything the server sends, the close of the connection included.

Usage: delaying_tcp_proxy.py <backend host> <backend port> <delay, seconds> <port file>

The port the proxy listens on is chosen by the OS and written to the port file (atomically, so a
non-empty file always contains the complete port). It is used by the tests that have to observe a
connection which the server has already closed but whose close has not been delivered yet.
"""

import os
import socket
import sys
import threading
import time

backend_host = sys.argv[1]
backend_port = int(sys.argv[2])
delay = float(sys.argv[3])
port_file = sys.argv[4]

listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", 0))
listener.listen(16)

with open(port_file + ".tmp", "w") as f:
    f.write(str(listener.getsockname()[1]))
os.rename(port_file + ".tmp", port_file)


def pump(src, dst, from_server):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            if from_server:
                time.sleep(delay)
            dst.sendall(data)
    except OSError:
        pass
    # The close is delayed as well: this is what makes a check of the socket alone insufficient.
    if from_server:
        time.sleep(delay)
    try:
        dst.shutdown(socket.SHUT_WR)
    except OSError:
        pass


def serve(client):
    try:
        server = socket.create_connection((backend_host, backend_port))
    except OSError:
        client.close()
        return
    threading.Thread(target=pump, args=(client, server, False), daemon=True).start()
    threading.Thread(target=pump, args=(server, client, True), daemon=True).start()


while True:
    connection, _ = listener.accept()
    threading.Thread(target=serve, args=(connection,), daemon=True).start()
