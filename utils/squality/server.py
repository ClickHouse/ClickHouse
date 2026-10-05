#!/usr/bin/env python3
"""Start a dedicated ClickHouse server for the SQuaLity experiment."""

import argparse
import http.client
from pathlib import Path
import subprocess
import time
import xml.etree.ElementTree as ET


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--http-port", type=int, default=18173)
    parser.add_argument("--tcp-port", type=int, default=19173)
    parser.add_argument("--max-memory", type=int, default=4294967296,
                        help="Server memory limit in bytes")
    args = parser.parse_args()
    directory = args.directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    config_path = directory / "config.xml"
    if config_path.exists():
        raise FileExistsError(f"Use a fresh server directory: {directory}")
    root = ET.Element("clickhouse")
    def add(parent, key, value):
        node = ET.SubElement(parent, key)
        node.text = str(value)
        return node
    for key, value in {
        "listen_host": "127.0.0.1", "http_port": args.http_port, "tcp_port": args.tcp_port,
        "path": str(directory / "data") + "/", "tmp_path": str(directory / "tmp") + "/",
        "user_files_path": str(directory / "user_files") + "/",
        "format_schema_path": str(directory / "format_schemas") + "/",
        "max_server_memory_usage": args.max_memory, "mark_cache_size": 67108864,
        "uncompressed_cache_size": 67108864,
        "background_pool_size": 16, "background_schedule_pool_size": 8,
        "background_move_pool_size": 2, "background_fetches_pool_size": 2,
        "background_common_pool_size": 2, "background_buffer_flush_schedule_pool_size": 2,
        "background_message_broker_schedule_pool_size": 2, "background_distributed_schedule_pool_size": 2
    }.items():
        add(root, key, value)
    logger = ET.SubElement(root, "logger")
    for key, value in {"level": "warning", "log": directory / "server.log",
                       "errorlog": directory / "server.err.log", "size": "20M", "count": 2}.items():
        add(logger, key, value)
    profiles = ET.SubElement(root, "profiles")
    profile = ET.SubElement(profiles, "default")
    add(profile, "max_threads", 2)
    users = ET.SubElement(root, "users")
    user = ET.SubElement(users, "default")
    add(user, "password", "")
    add(user, "profile", "default")
    add(user, "quota", "default")
    add(ET.SubElement(user, "networks"), "ip", "127.0.0.1")
    ET.SubElement(ET.SubElement(root, "quotas"), "default")
    ET.ElementTree(root).write(config_path, encoding="unicode")
    with (directory / "startup.log").open("w") as log:
        subprocess.run([str(args.binary.resolve()), "server", f"--config-file={config_path}",
                        f"--pid-file={directory / 'server.pid'}", "--daemon"],
                       check=True, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            connection = http.client.HTTPConnection("127.0.0.1", args.http_port, timeout=1)
            connection.request("POST", "/", body="SELECT version()")
            response = connection.getresponse()
            version = response.read().decode().strip()
            connection.close()
            if response.status == 200:
                print(f"Started ClickHouse {version} with config {config_path}")
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"ClickHouse did not start; inspect {directory / 'server.err.log'}")


if __name__ == "__main__":
    main()
