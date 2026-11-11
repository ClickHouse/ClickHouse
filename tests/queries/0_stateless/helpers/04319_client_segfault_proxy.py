#!/usr/bin/env python3
# Helper for `tests/queries/0_stateless/04319_client_segfault_send_error_105292.sh`.
#
# A single-shot TCP proxy that relays one client connection to a target server
# and forces a TCP RST on the client socket after `cutoff` bytes have flowed
# client -> server. The RST surfaces to clickhouse-client as ECONNRESET on its
# next write, which is exactly what `Connection::sendQuery` sees when a real
# server closes the stream mid-write (issue #105292).
#
# Exit code is the regression-path signal: 0 only when more than `cutoff` bytes
# were forwarded and the RST was injected; non-zero on every other path
# (target-connect failure, accept timeout, client EOF before cutoff, server
# send error before cutoff). The orchestrator must check this so a passing
# test always proves the mid-write cutoff path was hit.
#
# Usage: python3 04319_client_segfault_proxy.py <target_host> <target_port> <cutoff_bytes> <port_file>

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
    except OSError as e:
        print(f"TARGET_CONNECT_FAILED: {e}", flush=True)
        try:
            client_sock.close()
        except OSError:
            pass
        return False

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
    cutoff_reached = False
    client_eof = False
    client_recv_error = False
    server_send_error = False
    try:
        while True:
            try:
                data = client_sock.recv(8192)
            except OSError:
                client_recv_error = True
                break
            if not data:
                client_eof = True
                break
            try:
                server_sock.sendall(data)
            except OSError:
                server_send_error = True
                break
            sent += len(data)
            if sent > cutoff:
                cutoff_reached = True
                break
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

    if cutoff_reached:
        print(f"CUTOFF_EXCEEDED: bytes={sent} cutoff={cutoff}", flush=True)
        return True
    if client_eof:
        print(f"CLIENT_EOF_BEFORE_CUTOFF: bytes={sent} cutoff={cutoff}", flush=True)
    elif client_recv_error:
        print(f"CLIENT_RECV_ERROR_BEFORE_CUTOFF: bytes={sent} cutoff={cutoff}", flush=True)
    elif server_send_error:
        print(f"SERVER_SEND_ERROR_BEFORE_CUTOFF: bytes={sent} cutoff={cutoff}", flush=True)
    else:
        print(f"UNKNOWN_EARLY_EXIT: bytes={sent} cutoff={cutoff}", flush=True)
    return False


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
        print("ACCEPT_TIMEOUT", flush=True)
        return 1
    finally:
        listener.close()

    success = serve_one(client_sock, target_host, target_port, cutoff)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
