#!/usr/bin/env python3

import os
import random
import signal
import subprocess
import sys
import time
from argparse import ArgumentParser

XML_TEMPLATE = """
<clickhouse>
    <logger>
        <level>trace</level>
        <log>{data_dir}/clickhouse-keeper.log</log>
        <errorlog>{data_dir}/clickhouse-keeper.err.log</errorlog>
        <stderr>{data_dir}/stderr.log</stderr>
        <stdout>{data_dir}/stdout.log</stdout>
        <size>1000M</size>
        <count>10</count>
        <console>0</console>
    </logger>

    <keeper_server>
        <tcp_port>{client_port}</tcp_port>
        <server_id>{server_id}</server_id>
        <log_storage_path>{data_dir}/log</log_storage_path>
        <snapshot_storage_path>{data_dir}/snapshots</snapshot_storage_path>

        <coordination_settings>
            <operation_timeout_ms>5000</operation_timeout_ms>
            <session_timeout_ms>10000</session_timeout_ms>
            <raft_logs_level>trace</raft_logs_level>
            <snapshot_distance>1</snapshot_distance>
        </coordination_settings>

        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>localhost</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>2</id>
                <hostname>localhost</hostname>
                <port>9235</port>
            </server>
            <server>
                <id>3</id>
                <hostname>localhost</hostname>
                <port>9236</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>

"""


class Keeper:
    def __init__(
        self, keeper_binary_path, server_id, client_port, workdir, with_thread_fuzzer
    ):
        self.keeper_binary_path = keeper_binary_path
        if not os.path.exists(self.keeper_binary_path):
            raise Exception(
                f"Path for keeper binary doesn't exist {self.keeper_binary_path}"
            )
        self.server_id = server_id
        self.client_port = client_port
        self.workdir = workdir
        self.data_dir = os.path.join(self.workdir, f"data_server_{self.server_id}")
        self.config_dir = os.path.join(self.workdir, f"config_server_{self.server_id}")
        self.config_path = os.path.join(self.config_dir, "config.xml")
        self.with_thread_fuzzer = with_thread_fuzzer
        self.process = None

    def _create_dirs(self):
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)
        for p in [self.data_dir, self.config_dir]:
            if not os.path.exists(p):
                os.mkdir(p)

    def _prepare_and_write_config(self):
        if not os.path.exists(self.config_path):
            config = XML_TEMPLATE.format(
                client_port=self.client_port,
                server_id=self.server_id,
                data_dir=self.data_dir,
            )
            with open(self.config_path, "w", encoding="utf-8") as f:
                f.write(config)

    def prepare(self):
        self._create_dirs()
        self._prepare_and_write_config()

    def start(self):
        env = os.environ.copy()
        if self.with_thread_fuzzer:
            env["THREAD_FUZZER_CPU_TIME_PERIOD_US"] = "1000"
            env["THREAD_FUZZER_SLEEP_PROBABILITY"] = "0.1"
            env["THREAD_FUZZER_SLEEP_TIME_US_MAX"] = "100000"
            env["THREAD_FUZZER_pthread_mutex_lock_BEFORE_MIGRATE_PROBABILITY"] = "1"
            env["THREAD_FUZZER_pthread_mutex_lock_AFTER_MIGRATE_PROBABILITY"] = "1"
            env["THREAD_FUZZER_pthread_mutex_unlock_BEFORE_MIGRATE_PROBABILITY"] = "1"
            env["THREAD_FUZZER_pthread_mutex_unlock_AFTER_MIGRATE_PROBABILITY"] = "1"

            env["THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_PROBABILITY"] = "0.001"
            env["THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_PROBABILITY"] = "0.001"
            env["THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_PROBABILITY"] = "0.001"
            env["THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_PROBABILITY"] = "0.001"
            env["THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_TIME_US_MAX"] = "10000"
            env["THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_TIME_US_MAX"] = "10000"
            env["THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_TIME_US_MAX"] = "10000"
            env["THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_TIME_US_MAX"] = "10000"

        self.process = subprocess.Popen(
            [
                self.keeper_binary_path,
                "--config",
                self.config_path,
                "--log-file",
                f"{self.data_dir}/clickhouse-keeper.log",
                "--errorlog-file",
                f"{self.data_dir}/clickhouse-keeper.err.log",
            ],
            env=env,
        )

    def restart(self):
        print("Restarting keeper", self.server_id)
        self.stop()
        self.start()

    def stop(self):
        if self.process:
            self.process.kill()
            self.process = None


class KeeperBench:
    def __init__(self, bench_binary_path, client_ports):
        self.bench_binary_path = bench_binary_path
        self.client_ports = client_ports
        self.process = None

    def start(self):
        hosts = " ".join([f"--hosts localhost:{port}" for port in self.client_ports])
        cmd = f"{self.bench_binary_path} {hosts} --continue_on_errors -c 32 --generator create_small_data"
        self.process = subprocess.Popen(cmd, shell=True)

    def stop(self):
        if self.process:
            self.process.kill()
            self.process = None


def main(args):
    PORTS = [9181, 9182, 9183]
    SERVER_IDS = [1, 2, 3]
    workdir = args.workdir
    keeper_binary_path = args.keeper_binary_path
    keeper_bench_path = args.keeper_bench_path

    keepers = []
    for port, server_id in zip(PORTS, SERVER_IDS):
        keepers.append(
            Keeper(
                keeper_binary_path, server_id, port, workdir, args.with_thread_fuzzer
            )
        )

    bench = KeeperBench(keeper_bench_path, PORTS)

    for keeper in keepers:
        keeper.prepare()
        keeper.start()

    time.sleep(10)

    bench.start()

    def signal_handler(sig, frame):
        print("You pressed Ctrl+C!")
        for keeper in keepers:
            keeper.stop()

        bench.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    for _ in range(100):
        time.sleep(30)
        sacrifice = random.choice(keepers)
        sacrifice.restart()
        time.sleep(60)


if __name__ == "__main__":
    parser = ArgumentParser(description="Simple tool to load three-nodes keeper")
    parser.add_argument(
        "--workdir", default="./keeper-overload-workdir", help="Path to workdir"
    )
    parser.add_argument(
        "--keeper-binary-path", required=True, help="Path to keeper binary"
    )
    parser.add_argument(
        "--keeper-bench-path", required=True, help="Path to keeper bench utility"
    )
    parser.add_argument(
        "--with-thread-fuzzer", action="store_true", help="Path to keeper bench utility"
    )

    args = parser.parse_args()

    main(args)
