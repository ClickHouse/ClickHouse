#!/usr/bin/env python3
"""Generates `programs/startup_symbol_order.txt`: the functions that ClickHouse executes at startup, in the
order of their first execution, for `--symbol-ordering-file` of lld. See README.md.

Usage: generate.py <clickhouse binary with a symbol table> [output file] [directory with config.xml and users.xml]

The configuration defaults to `programs/server` of this repository; it must be of the same version as the binary.
"""

import os
import signal
import socket
import sys
import tempfile
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from first_execution import trace  # noqa: E402

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))


def free_ports(count):
    sockets = []
    for _ in range(count):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        sockets.append(s)
    ports = [s.getsockname()[1] for s in sockets]
    for s in sockets:
        s.close()
    return ports


def trace_local(binary, workdir):
    """`clickhouse local` running a trivial query."""
    home = os.path.join(workdir, "local")
    os.makedirs(home)
    env = dict(os.environ, HOME=home)
    return trace(binary, ["local", "--query", "SELECT 1"], env=env, cwd=home)


def trace_server(binary, workdir, config_source):
    """`clickhouse server` with the packaged configuration: the startup up to accepting connections,
    a trivial query over HTTP, and the shutdown."""
    root = os.path.join(workdir, "server")
    config_dir = os.path.join(root, "config")
    os.makedirs(os.path.join(config_dir, "config.d"))
    for name in ("config.xml", "users.xml"):
        with open(os.path.join(config_source, name)) as src, open(os.path.join(config_dir, name), "w") as dst:
            dst.write(src.read())

    http, tcp, mysql, postgresql, interserver = free_ports(5)
    data = os.path.join(root, "data")
    with open(os.path.join(config_dir, "config.d", "startup_symbol_order.xml"), "w") as f:
        f.write(f"""<clickhouse>
    <logger>
        <log>{root}/log/clickhouse-server.log</log>
        <errorlog>{root}/log/clickhouse-server.err.log</errorlog>
    </logger>
    <listen_host>127.0.0.1</listen_host>
    <http_port>{http}</http_port>
    <tcp_port>{tcp}</tcp_port>
    <mysql_port>{mysql}</mysql_port>
    <postgresql_port>{postgresql}</postgresql_port>
    <interserver_http_port>{interserver}</interserver_http_port>
    <path>{data}/</path>
    <tmp_path>{data}/tmp/</tmp_path>
    <user_files_path>{data}/user_files/</user_files_path>
    <format_schema_path>{data}/format_schemas/</format_schema_path>
    <custom_cached_disks_base_directory>{data}/caches/</custom_cached_disks_base_directory>
    <user_directories>
        <local_directory>
            <path>{data}/access/</path>
        </local_directory>
    </user_directories>
    <!-- The breakpoints of the tracer modify the code. -->
    <skip_binary_checksum_checks>true</skip_binary_checksum_checks>
    <mlock_executable>false</mlock_executable>
</clickhouse>
""")

    def query(text):
        request = urllib.request.Request(f"http://127.0.0.1:{http}/", data=text.encode())
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.read()

    ready = []

    def drive(pid):
        """Waits until the server accepts queries, runs one, and stops the server."""
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            try:
                if query("SELECT 1").strip() == b"1":
                    ready.append(True)
                    break
            except OSError:
                pass
            time.sleep(0.1)
        os.kill(pid, signal.SIGTERM)

    # The watchdog would fork the server, and the child would inherit the breakpoints without a tracer.
    env = dict(os.environ, CLICKHOUSE_WATCHDOG_ENABLE="0")
    order = trace(binary, ["server", "--config-file", os.path.join(config_dir, "config.xml")],
                  env=env, cwd=root, on_started=drive)
    if not ready:
        raise RuntimeError(f"the server did not start, see the logs in {root}/log")
    return order


def main():
    if len(sys.argv) not in (2, 3, 4):
        sys.exit(__doc__)
    binary = os.path.abspath(sys.argv[1])
    output = sys.argv[2] if len(sys.argv) >= 3 else os.path.join(REPO, "programs", "startup_symbol_order.txt")
    config_source = sys.argv[3] if len(sys.argv) == 4 else os.path.join(REPO, "programs", "server")

    with tempfile.TemporaryDirectory() as workdir:
        local = trace_local(binary, workdir)
        server = trace_server(binary, workdir, config_source)

    # `clickhouse local` first: its functions are a prefix of what the server runs, mostly.
    order = dict.fromkeys(local + server)
    with open(output, "w") as f:
        f.write("".join(name + "\n" for name in order))
    print(f"{len(dict.fromkeys(local))} functions for `clickhouse local`, {len(dict.fromkeys(server))} for the server, "
          f"{len(order)} in total -> {output}")


if __name__ == "__main__":
    main()
