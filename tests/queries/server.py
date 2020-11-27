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
        self.dicts_config = os.path.join(self.etc_dir, 'dictionaries.xml')
        self.client_config = os.path.join(self.etc_dir, 'client-config.xml')

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
            # TODO: SSL certificate is not specified '--tcp_port_secure={tcps_port}'.format(tcps_port=self.tcps_port),
        ]

        with open(self.server_config, 'w') as f:
            f.write(ServerThread.DEFAULT_SERVER_CONFIG.format(
                tmp_dir=self.tmp_dir, log_dir=self.log_dir, tcp_port=self.tcp_port))

        with open(self.users_config, 'w') as f:
            f.write(ServerThread.DEFAULT_USERS_CONFIG)

        with open(self.dicts_config, 'w') as f:
            f.write(ServerThread.DEFAULT_DICTIONARIES_CONFIG.format(tcp_port=self.tcp_port))

        with open(self.client_config, 'w') as f:
            f.write(ServerThread.DEFAULT_CLIENT_CONFIG)

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
                    s.sendall(b'G')  # trigger expected "bad" HELLO response
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
                print(stdout.decode('utf-8'), file=sys.stderr)
                print(stderr.decode('utf-8'), file=sys.stderr)

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
    <dictionaries_config>dictionaries.xml</dictionaries_config>
    <mark_cache_size>5368709120</mark_cache_size>

    <keep_alive_timeout>3</keep_alive_timeout>
    <timezone>Europe/Moscow</timezone>

    <macros>
        <test>Hello, world!</test>
        <shard>s1</shard>
        <replica>r1</replica>
    </macros>

    <graphite_rollup>
        <version_column_name>Version</version_column_name>
        <pattern>
            <regexp>sum</regexp>
            <function>sum</function>
            <retention>
                <age>0</age>
                <precision>600</precision>
            </retention>
            <retention>
                <age>172800</age> <!-- 2 days -->
                <precision>6000</precision>
            </retention>
        </pattern>
        <default>
            <function>max</function>
            <retention>
                <age>0</age>
                <precision>600</precision>
            </retention>
            <retention>
                <age>172800</age>
                <precision>6000</precision>
            </retention>
        </default>
    </graphite_rollup>

    <query_masking_rules>
        <rule>
            <regexp>TOPSECRET.TOPSECRET</regexp>
            <replace>[hidden]</replace>
        </rule>
    </query_masking_rules>

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

        <test_cluster_two_shards>
            <shard>
                <replica>
                    <host>127.0.0.1</host>
                    <port>{tcp_port}</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>127.0.0.2</host>
                    <port>{tcp_port}</port>
                </replica>
            </shard>
        </test_cluster_two_shards>

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

        <test_cluster_two_shards_different_databases>
             <shard>
                 <replica>
                     <default_database>shard_0</default_database>
                     <host>localhost</host>
                     <port>{tcp_port}</port>
                 </replica>
             </shard>
             <shard>
                 <replica>
                     <default_database>shard_1</default_database>
                     <host>localhost</host>
                     <port>{tcp_port}</port>
                 </replica>
             </shard>
         </test_cluster_two_shards_different_databases>

         <test_cluster_two_shards_internal_replication>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>127.0.0.1</host>
                    <port>{tcp_port}</port>
                </replica>
            </shard>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>127.0.0.2</host>
                    <port>{tcp_port}</port>
                </replica>
            </shard>
        </test_cluster_two_shards_internal_replication>

        <test_cluster_with_incorrect_pw>
             <shard>
                 <internal_replication>true</internal_replication>
                 <replica>
                     <host>127.0.0.1</host>
                     <port>{tcp_port}</port>
                     <!-- password is incorrect -->
                     <password>foo</password>
                 </replica>
                 <replica>
                     <host>127.0.0.2</host>
                     <port>{tcp_port}</port>
                     <!-- password is incorrect -->
                     <password>foo</password>
                 </replica>
             </shard>
         </test_cluster_with_incorrect_pw>
    </remote_servers>

    <storage_configuration>
        <disks>
            <disk_memory>
                <type>memory</type>
            </disk_memory>
        </disks>
    </storage_configuration>

    <zookeeper>
        <implementation>testkeeper</implementation>
    </zookeeper>

    <part_log>
        <database>system</database>
        <table>part_log</table>
    </part_log>

    <query_log>
        <database>system</database>
        <table>query_log</table>
    </query_log>

    <query_thread_log>
        <database>system</database>
        <table>query_thread_log</table>
    </query_thread_log>

    <text_log>
        <database>system</database>
        <table>text_log</table>
    </text_log>

    <trace_log>
        <database>system</database>
        <table>trace_log</table>
    </trace_log>
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


ServerThread.DEFAULT_DICTIONARIES_CONFIG = \
"""\
<yandex>
    <dictionary>
        <name>flat_ints</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>ints</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <flat/>
        </layout>
        <structure>
            <id>
                <name>key</name>
            </id>
            <attribute>
                <name>i8</name>
                <type>Int8</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i16</name>
                <type>Int16</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i32</name>
                <type>Int32</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i64</name>
                <type>Int64</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u8</name>
                <type>UInt8</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u16</name>
                <type>UInt16</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u32</name>
                <type>UInt32</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u64</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>hashed_ints</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>ints</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <hashed/>
        </layout>
        <structure>
            <id>
                <name>key</name>
            </id>
            <attribute>
                <name>i8</name>
                <type>Int8</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i16</name>
                <type>Int16</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i32</name>
                <type>Int32</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i64</name>
                <type>Int64</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u8</name>
                <type>UInt8</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u16</name>
                <type>UInt16</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u32</name>
                <type>UInt32</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u64</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>hashed_sparse_ints</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>ints</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <sparse_hashed/>
        </layout>
        <structure>
            <id>
                <name>key</name>
            </id>
            <attribute>
                <name>i8</name>
                <type>Int8</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i16</name>
                <type>Int16</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i32</name>
                <type>Int32</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i64</name>
                <type>Int64</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u8</name>
                <type>UInt8</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u16</name>
                <type>UInt16</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u32</name>
                <type>UInt32</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u64</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>cache_ints</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>ints</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <cache><size_in_cells>1000</size_in_cells></cache>
        </layout>
        <structure>
            <id>
                <name>key</name>
            </id>
            <attribute>
                <name>i8</name>
                <type>Int8</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i16</name>
                <type>Int16</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i32</name>
                <type>Int32</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i64</name>
                <type>Int64</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u8</name>
                <type>UInt8</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u16</name>
                <type>UInt16</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u32</name>
                <type>UInt32</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u64</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>complex_hashed_ints</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>ints</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <complex_key_hashed/>
        </layout>
        <structure>
            <key>
                <attribute>
                    <name>key</name>
                    <type>UInt64</type>
                </attribute>
            </key>
            <attribute>
                <name>i8</name>
                <type>Int8</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i16</name>
                <type>Int16</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i32</name>
                <type>Int32</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i64</name>
                <type>Int64</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u8</name>
                <type>UInt8</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u16</name>
                <type>UInt16</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u32</name>
                <type>UInt32</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u64</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>complex_cache_ints</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>ints</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <complex_key_cache><size_in_cells>1000</size_in_cells></complex_key_cache>
        </layout>
        <structure>
            <key>
                <attribute>
                    <name>key</name>
                    <type>UInt64</type>
                </attribute>
            </key>
            <attribute>
                <name>i8</name>
                <type>Int8</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i16</name>
                <type>Int16</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i32</name>
                <type>Int32</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>i64</name>
                <type>Int64</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u8</name>
                <type>UInt8</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u16</name>
                <type>UInt16</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u32</name>
                <type>UInt32</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>u64</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>flat_strings</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>strings</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <flat/>
        </layout>
        <structure>
            <id>
                <name>key</name>
            </id>
            <attribute>
                <name>str</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>hashed_strings</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>strings</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <hashed/>
        </layout>
        <structure>
            <id>
                <name>key</name>
            </id>
            <attribute>
                <name>str</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>cache_strings</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>strings</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <cache><size_in_cells>1000</size_in_cells></cache>
        </layout>
        <structure>
            <id>
                <name>key</name>
            </id>
            <attribute>
                <name>str</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>complex_hashed_strings</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>strings</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <complex_key_hashed/>
        </layout>
        <structure>
            <key>
                <attribute>
                    <name>key</name>
                    <type>UInt64</type>
                </attribute>
            </key>
            <attribute>
                <name>str</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>complex_cache_strings</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>strings</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <complex_key_cache><size_in_cells>1000</size_in_cells></complex_key_cache>
        </layout>
        <structure>
            <key>
                <attribute>
                    <name>key</name>
                    <type>UInt64</type>
                </attribute>
            </key>
            <attribute>
                <name>str</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>flat_decimals</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>decimals</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <flat/>
        </layout>
        <structure>
            <id>
                <name>key</name>
            </id>
            <attribute>
                <name>d32</name>
                <type>Decimal32(4)</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>d64</name>
                <type>Decimal64(6)</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>d128</name>
                <type>Decimal128(1)</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>hashed_decimals</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>decimals</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <hashed/>
        </layout>
        <structure>
            <id>
                <name>key</name>
            </id>
            <attribute>
                <name>d32</name>
                <type>Decimal32(4)</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>d64</name>
                <type>Decimal64(6)</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>d128</name>
                <type>Decimal128(1)</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>cache_decimals</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>decimals</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <cache><size_in_cells>1000</size_in_cells></cache>
        </layout>
        <structure>
            <id>
                <name>key</name>
            </id>
            <attribute>
                <name>d32</name>
                <type>Decimal32(4)</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>d64</name>
                <type>Decimal64(6)</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>d128</name>
                <type>Decimal128(1)</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>complex_hashed_decimals</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>decimals</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <complex_key_hashed/>
        </layout>
        <structure>
            <key>
                <attribute>
                    <name>key</name>
                    <type>UInt64</type>
                </attribute>
            </key>
            <attribute>
                <name>d32</name>
                <type>Decimal32(4)</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>d64</name>
                <type>Decimal64(6)</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>d128</name>
                <type>Decimal128(1)</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>

    <dictionary>
        <name>complex_cache_decimals</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>{tcp_port}</port>
                <user>default</user>
                <password></password>
                <db>system</db>
                <table>decimals</table>
            </clickhouse>
        </source>
        <lifetime>0</lifetime>
        <layout>
            <complex_key_cache><size_in_cells>1000</size_in_cells></complex_key_cache>
        </layout>
        <structure>
            <key>
                <attribute>
                    <name>key</name>
                    <type>UInt64</type>
                </attribute>
            </key>
            <attribute>
                <name>d32</name>
                <type>Decimal32(4)</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>d64</name>
                <type>Decimal64(6)</type>
                <null_value>0</null_value>
            </attribute>
            <attribute>
                <name>d128</name>
                <type>Decimal128(1)</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>
</yandex>
"""

ServerThread.DEFAULT_CLIENT_CONFIG = \
"""\
<config>
    <openSSL>
        <client>
            <loadDefaultCAFile>true</loadDefaultCAFile>
            <cacheSessions>true</cacheSessions>
            <disableProtocols>sslv2,sslv3</disableProtocols>
            <preferServerCiphers>true</preferServerCiphers>
            <invalidCertificateHandler>
                <name>AcceptCertificateHandler</name>
            </invalidCertificateHandler>
        </client>
    </openSSL>
</config>
"""
