#!/usr/bin/env python3
# Tags: no-fasttest

import os
import socket
import subprocess
import tempfile
from pathlib import Path


with tempfile.TemporaryDirectory() as directory, socket.socket() as occupied:
    occupied.bind(("127.0.0.1", 0))
    occupied.listen()
    raft_port = occupied.getsockname()[1]

    with socket.socket() as client:
        client.bind(("127.0.0.1", 0))
        client_port = client.getsockname()[1]

    config = Path(directory) / "keeper.xml"
    config.write_text(
        f"""<clickhouse>
    <logger><console>1</console></logger>
    <listen_host>127.0.0.1</listen_host>
    <interserver_listen_host>127.0.0.1</interserver_listen_host>
    <keeper_server>
        <tcp_port>{client_port}</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>{directory}/log</log_storage_path>
        <snapshot_storage_path>{directory}/snapshots</snapshot_storage_path>
        <raft_configuration>
            <server><id>1</id><hostname>127.0.0.1</hostname><port>{raft_port}</port></server>
        </raft_configuration>
    </keeper_server>
</clickhouse>"""
    )
    result = subprocess.run(
        [os.environ.get("CLICKHOUSE_BINARY", "clickhouse"), "keeper", f"--config={config}"],
        cwd=directory,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0, result.stdout
    assert "Cannot listen on any configured Raft host" in result.stdout, result.stdout
