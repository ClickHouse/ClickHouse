import os
import random
import socket
import subprocess
import sys
import threading
import time


class ServerThread(threading.Thread):
    DEFAULT_RETRIES = 3
    DEFAULT_SERVER_DELAY = 0.5  # seconds
    DEFAULT_CONNECTION_TIMEOUT = 1.0  # seconds

    def __init__(self, bin_prefix, tmp_dir):
        self._bin = bin_prefix + '-server'
        self._lock = threading.Lock()
        threading.Thread.__init__(self)
        self._lock.acquire()

        self.tmp_dir = tmp_dir
        self.log_dir = os.path.join(tmp_dir, 'log')
        self.etc_dir = os.path.join(tmp_dir, 'etc')
        self.server_config = os.path.join(self.etc_dir, 'server-config.xml')
        self.users_config = os.path.join(self.etc_dir, 'users.xml')

        os.makedirs(self.log_dir)
        os.makedirs(self.etc_dir)

    def _choose_ports_and_args(self):
        port_base = random.SystemRandom().randrange(10000, 60000)
        self.tcp_port = port_base + 1
        self.http_port = port_base + 2
        self.inter_port = port_base + 3
        self.tcps_port = port_base + 4
        self.https_port = port_base + 5
        self.odbc_port = port_base + 6

        self._args = [
            '--config-file={config_path}'.format(config_path=self.server_config),
            '--',
            '--tcp_port={tcp_port}'.format(tcp_port=self.tcp_port),
            '--http_port={http_port}'.format(http_port=self.http_port),
            '--interserver_http_port={inter_port}'.format(inter_port=self.inter_port),
        ]

        with open(self.server_config, 'w') as f:
            f.write(ServerThread.DEFAULT_SERVER_CONFIG.format(
                tmp_dir=self.tmp_dir, log_dir=self.log_dir, tcp_port=self.tcp_port))

        with open(self.users_config, 'w') as f:
            f.write(ServerThread.DEFAULT_USERS_CONFIG)

    def run(self):
        retries = ServerThread.DEFAULT_RETRIES

        while retries:
            self._choose_ports_and_args()
            print('Start clickhouse-server with args:', self._args)
            self._proc = subprocess.Popen([self._bin] + self._args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            while self._proc.poll() is None:
                try:
                    time.sleep(ServerThread.DEFAULT_SERVER_DELAY)
                    s = socket.create_connection(('localhost', self.tcp_port), ServerThread.DEFAULT_CONNECTION_TIMEOUT)
                    s.sendall('G')  # trigger expected "bad" HELLO response
                    print('Successful server response:', s.recv(1024))  # FIXME: read whole buffered response
                    s.shutdown(socket.SHUT_RDWR)
                    s.close()
                except Exception as e:
                    print('Failed to connect to server:', e, file=sys.stderr)
                    continue
                else:
                    break

            # If process has died then try to fetch output before releasing lock
            if self._proc.returncode is not None:
                stdout, stderr = self._proc.communicate()
                print(stdout, file=sys.stderr)
                print(stderr, file=sys.stderr)

            if self._proc.returncode == 70:  # Address already in use
                retries -= 1
                continue

            break

        self._lock.release()

        while self._proc.returncode is None:
            self._proc.communicate()

    def wait(self):
        self._lock.acquire()
        if self._proc.returncode is not None:
            self.join()
        self._lock.release()
        return self._proc.returncode

    def stop(self):
        if self._proc.returncode is None:
            self._proc.terminate()
        self.join()
        print('Stop clickhouse-server')


ServerThread.DEFAULT_SERVER_CONFIG = \
"""\
<?xml version="1.0"?>
<yandex>
    <logger>
        <level>trace</level>
        <log>{log_dir}/server/stdout.txt</log>
        <errorlog>{log_dir}/server/stderr.txt</errorlog>
        <size>never</size>
        <count>1</count>
    </logger>

    <listen_host>::</listen_host>

    <path>{tmp_dir}/data/</path>
    <tmp_path>{tmp_dir}/tmp/</tmp_path>
    <access_control_path>{tmp_dir}/data/access/</access_control_path>
    <users_config>users.xml</users_config>
    <mark_cache_size>5368709120</mark_cache_size>

    <timezone>Europe/Moscow</timezone>

    <remote_servers>
        <test_shard_localhost>
            <shard>
                <replica>
                    <host>localhost</host>
                    <port>{tcp_port}</port>
                </replica>
            </shard>
        </test_shard_localhost>

        <test_cluster_two_shards_localhost>
             <shard>
                 <replica>
                     <host>localhost</host>
                     <port>{tcp_port}</port>
                 </replica>
             </shard>
             <shard>
                 <replica>
                     <host>localhost</host>
                     <port>{tcp_port}</port>
                 </replica>
             </shard>
         </test_cluster_two_shards_localhost>

        <test_unavailable_shard>
            <shard>
                <replica>
                    <host>localhost</host>
                    <port>{tcp_port}</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>localhost</host>
                    <port>1</port>
                </replica>
            </shard>
        </test_unavailable_shard>
    </remote_servers>
</yandex>
"""


ServerThread.DEFAULT_USERS_CONFIG = \
"""\
<?xml version="1.0"?>
<yandex>
    <profiles>
        <default>
            <max_memory_usage>10000000000</max_memory_usage>
            <use_uncompressed_cache>0</use_uncompressed_cache>
            <load_balancing>random</load_balancing>
        </default>

        <readonly>
            <readonly>1</readonly>
        </readonly>
    </profiles>

    <users>
        <default>
            <password></password>
            <networks replace="replace">
                <ip>::/0</ip>
            </networks>

            <profile>default</profile>

            <quota>default</quota>

            <access_management>1</access_management>
        </default>

        <readonly>
            <password></password>
            <networks replace="replace">
                <ip>::1</ip>
                <ip>127.0.0.1</ip>
            </networks>

            <profile>readonly</profile>

            <quota>default</quota>
        </readonly>
    </users>

    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>
</yandex>
"""
