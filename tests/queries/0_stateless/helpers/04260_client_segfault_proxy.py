#!/usr/bin/env python3
# Helper for `tests/queries/0_stateless/04260_client_segfault_send_error_105292.sh`.
#
# A single-shot TCP proxy that relays one client connection to a target server
# and forces a TCP RST on the client socket after `cutoff` bytes have flowed
# client -> server. The RST surfaces to clickhouse-client as ECONNRESET on its
# next write, which is exactly what `Connection::sendQuery` sees when a real
# server closes the stream mid-write (issue #105292).
#
# Usage: python3 04260_client_segfault_proxy.py <target_host> <target_port> <cutoff_bytes> <port_file>
#
# The proxy writes the listening port atomically to <port_file>, services one
# connection, and exits. No external kill is needed and no orphan job remains.

import os
import socket
import struct
import sys
import threading


def force_rst(sock):
    # SO_LINGER with timeout 0 makes close() emit a TCP RST instead of FIN,
    # so the client observes ECONNRESET immediately (loopback send buffers
    # can otherwise absorb several MB before the failure surfaces).
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    except OSError:
        pass


def serve_one(client_sock, target_host, target_port, cutoff):
    try:
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.connect((target_host, target_port))
    except OSError:
        client_sock.close()
        return

    cutoff_event = threading.Event()

    def s2c():
        try:
            while not cutoff_event.is_set():
                data = server_sock.recv(8192)
                if not data:
                    break
                try:
                    client_sock.sendall(data)
                except OSError:
                    break
        except OSError:
            pass

    threading.Thread(target=s2c, daemon=True).start()

    sent = 0
    try:
        while True:
            data = client_sock.recv(8192)
            if not data:
                break
            try:
                server_sock.sendall(data)
            except OSError:
                break
            sent += len(data)
            if sent > cutoff:
                break
    except OSError:
        pass
    finally:
        cutoff_event.set()
        force_rst(client_sock)
        force_rst(server_sock)
        try:
            client_sock.close()
        except OSError:
            pass
        try:
            server_sock.close()
        except OSError:
            pass


def main():
    target_host = sys.argv[1]
    target_port = int(sys.argv[2])
    cutoff = int(sys.argv[3])
    port_file = sys.argv[4]

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    # Atomic publish so the orchestrator can read the port without races.
    tmp = port_file + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(port))
    os.rename(tmp, port_file)

    # Bound the proxy lifetime so a hung client cannot wedge the test forever.
    listener.settimeout(120)
    try:
        client_sock, _ = listener.accept()
    except socket.timeout:
        return
    finally:
        listener.close()

    serve_one(client_sock, target_host, target_port, cutoff)


if __name__ == "__main__":
    main()
