## Download

Cloning whole repo will take a lot of time and disk space. The following commands will download only this directory.

* Requires Git 2.19

```
# mkdir chdiag
# cd chdiag
# git clone --depth 1 --filter=blob:none --no-checkout https://github.com/ClickHouse/ClickHouse
# cd ClickHouse
# git sparse-checkout set utils/clickhouse-diagnostics
# git checkout master -- utils/clickhouse-diagnostics
```

## Installation

```
python3 -m pip install -r requirements.txt
```

## Usage

```
./clickhouse-diagnostics --host localhost --port 8123 --user default --password xxx
```

Example output:

### Diagnostics data for host clickhouse01.test_net_3697
Version: **21.11.8.4**
Timestamp: **2021-12-25 15:34:02**
Uptime: **13 minutes and 51 seconds**
#### ClickHouse configuration
**result**
```XML
<clickhouse>
	<logger>
		<level>trace</level>
		<log>/var/log/clickhouse-server/clickhouse-server.log</log>
		<errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
		<size>1000M</size>
		<count>10</count>
		<console>1</console>
	</logger>
	<http_port>8123</http_port>
	<tcp_port>9000</tcp_port>
	<mysql_port>9004</mysql_port>
	<postgresql_port>9005</postgresql_port>
	<interserver_http_port>9009</interserver_http_port>
	<max_connections>4096</max_connections>
	<keep_alive_timeout>3</keep_alive_timeout>
	<grpc>
		<enable_ssl>false</enable_ssl>
		<ssl_cert_file>/path/to/ssl_cert_file</ssl_cert_file>
		<ssl_key_file>/path/to/ssl_key_file</ssl_key_file>
		<ssl_require_client_auth>false</ssl_require_client_auth>
		<ssl_ca_cert_file>/path/to/ssl_ca_cert_file</ssl_ca_cert_file>
		<compression>deflate</compression>
		<compression_level>medium</compression_level>
		<max_send_message_size>-1</max_send_message_size>
		<max_receive_message_size>-1</max_receive_message_size>
		<verbose_logs>false</verbose_logs>
	</grpc>
	<openSSL>
		<server>
			<certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
			<privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
			<dhParamsFile></dhParamsFile>
			<verificationMode>none</verificationMode>
			<loadDefaultCAFile>true</loadDefaultCAFile>
			<cacheSessions>true</cacheSessions>
			<disableProtocols>sslv2,sslv3</disableProtocols>
			<preferServerCiphers>true</preferServerCiphers>
		</server>
		<client>
			<loadDefaultCAFile>true</loadDefaultCAFile>
			<cacheSessions>true</cacheSessions>
			<disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
			<preferServerCiphers>true</preferServerCiphers>
			<invalidCertificateHandler>
				<name>RejectCertificateHandler</name>
			</invalidCertificateHandler>
			<certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
			<privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
			<caConfig>/etc/clickhouse-server/allCAs.pem</caConfig>
		</client>
	</openSSL>
	<max_concurrent_queries>100</max_concurrent_queries>
	<max_server_memory_usage>0</max_server_memory_usage>
	<max_thread_pool_size>10000</max_thread_pool_size>
	<max_server_memory_usage_to_ram_ratio>0.9</max_server_memory_usage_to_ram_ratio>
	<total_memory_profiler_step>4194304</total_memory_profiler_step>
	<total_memory_tracker_sample_probability>0</total_memory_tracker_sample_probability>
	<uncompressed_cache_size>8589934592</uncompressed_cache_size>
	<mark_cache_size>5368709120</mark_cache_size>
	<mmap_cache_size>1000</mmap_cache_size>
	<compiled_expression_cache_size>134217728</compiled_expression_cache_size>
	<compiled_expression_cache_elements_size>10000</compiled_expression_cache_elements_size>
	<path>/var/lib/clickhouse/</path>
	<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
	<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
	<ldap_servers></ldap_servers>
	<user_directories>
		<users_xml>
			<path>users.xml</path>
		</users_xml>
		<local_directory>
			<path>/var/lib/clickhouse/access/</path>
		</local_directory>
	</user_directories>
	<default_profile>default</default_profile>
	<custom_settings_prefixes></custom_settings_prefixes>
	<default_database>default</default_database>
	<mlock_executable>true</mlock_executable>
	<remap_executable>false</remap_executable>
	<remote_servers>
		<cluster_name>
			<shard>
				<replica>
					<host>clickhouse01.test_net_3697</host>
					<port>9000</port>
				</replica>
			</shard>
		</cluster_name>
	</remote_servers>
	<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
	<max_session_timeout>3600</max_session_timeout>
	<default_session_timeout>60</default_session_timeout>
	<query_log>
		<database>system</database>
		<table>query_log</table>
		<partition_by>toYYYYMM(event_date)</partition_by>
		<flush_interval_milliseconds>7500</flush_interval_milliseconds>
	</query_log>
	<trace_log>
		<database>system</database>
		<table>trace_log</table>
		<partition_by>toYYYYMM(event_date)</partition_by>
		<flush_interval_milliseconds>7500</flush_interval_milliseconds>
	</trace_log>
	<query_thread_log>
		<database>system</database>
		<table>query_thread_log</table>
		<partition_by>toYYYYMM(event_date)</partition_by>
		<flush_interval_milliseconds>7500</flush_interval_milliseconds>
	</query_thread_log>
	<query_views_log>
		<database>system</database>
		<table>query_views_log</table>
		<partition_by>toYYYYMM(event_date)</partition_by>
		<flush_interval_milliseconds>7500</flush_interval_milliseconds>
	</query_views_log>
	<part_log>
		<database>system</database>
		<table>part_log</table>
		<partition_by>toYYYYMM(event_date)</partition_by>
		<flush_interval_milliseconds>7500</flush_interval_milliseconds>
	</part_log>
	<metric_log>
		<database>system</database>
		<table>metric_log</table>
		<flush_interval_milliseconds>7500</flush_interval_milliseconds>
		<collect_interval_milliseconds>1000</collect_interval_milliseconds>
	</metric_log>
	<asynchronous_metric_log>
		<database>system</database>
		<table>asynchronous_metric_log</table>
		<flush_interval_milliseconds>7000</flush_interval_milliseconds>
	</asynchronous_metric_log>
	<opentelemetry_span_log>
		<engine>engine MergeTree
            partition by toYYYYMM(finish_date)
            order by (finish_date, finish_time_us, trace_id)</engine>
		<database>system</database>
		<table>opentelemetry_span_log</table>
		<flush_interval_milliseconds>7500</flush_interval_milliseconds>
	</opentelemetry_span_log>
	<crash_log>
		<database>system</database>
		<table>crash_log</table>
		<partition_by></partition_by>
		<flush_interval_milliseconds>1000</flush_interval_milliseconds>
	</crash_log>
	<session_log>
		<database>system</database>
		<table>session_log</table>
		<partition_by>toYYYYMM(event_date)</partition_by>
		<flush_interval_milliseconds>7500</flush_interval_milliseconds>
	</session_log>
	<top_level_domains_lists></top_level_domains_lists>
	<dictionaries_config>*_dictionary.xml</dictionaries_config>
	<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
	<encryption_codecs></encryption_codecs>
	<distributed_ddl>
		<path>/clickhouse/task_queue/ddl</path>
	</distributed_ddl>
	<graphite_rollup_example>
		<pattern>
			<regexp>click_cost</regexp>
			<function>any</function>
			<retention>
				<age>0</age>
				<precision>3600</precision>
			</retention>
			<retention>
				<age>86400</age>
				<precision>60</precision>
			</retention>
		</pattern>
		<default>
			<function>max</function>
			<retention>
				<age>0</age>
				<precision>60</precision>
			</retention>
			<retention>
				<age>3600</age>
				<precision>300</precision>
			</retention>
			<retention>
				<age>86400</age>
				<precision>3600</precision>
			</retention>
		</default>
	</graphite_rollup_example>
	<format_schema_path>/var/lib/clickhouse/format_schemas/</format_schema_path>
	<query_masking_rules>
		<rule>
			<name>hide encrypt/decrypt arguments</name>
			<regexp>((?:aes_)?(?:encrypt|decrypt)(?:_mysql)?)\s*\(\s*(?:'(?:\\'|.)+'|.*?)\s*\)</regexp>
			<replace>\1(???)</replace>
		</rule>
	</query_masking_rules>
	<send_crash_reports>
		<enabled>false</enabled>
		<anonymize>false</anonymize>
		<endpoint>https://6f33034cfe684dd7a3ab9875e57b1c8d@o388870.ingest.sentry.io/5226277</endpoint>
	</send_crash_reports>
	<listen_host>0.0.0.0</listen_host>
	<https_port>8443</https_port>
	<tcp_ssl_port>9440</tcp_ssl_port>
	<zookeeper>
		<node index="1">
			<host>zookeeper01.test_net_3697</host>
			<port>2281</port>
			<secure>1</secure>
		</node>
		<session_timeout_ms>3000</session_timeout_ms>
		<root>/clickhouse01</root>
		<identity>*****</identity>
	</zookeeper>
	<macros>
		<replica>clickhouse01</replica>
		<shard>shard1</shard>
	</macros>
	<database_atomic_delay_before_drop_table_sec>0</database_atomic_delay_before_drop_table_sec>
	<storage_configuration>
		<disks>
			<hdd1>
				<path>/hdd1/</path>
			</hdd1>
			<hdd2>
				<path>/hdd2/</path>
			</hdd2>
			<s3>
				<type>s3</type>
				<endpoint>http://minio01:9000/cloud-storage-01/data/</endpoint>
				<access_key_id>bB5vT2M8yaRv9J14SnAP</access_key_id>
				<secret_access_key>*****</secret_access_key>
				<send_metadata>true</send_metadata>
			</s3>
		</disks>
		<policies>
			<multiple_disks>
				<volumes>
					<main>
						<disk>default</disk>
					</main>
					<hdd1>
						<disk>hdd1</disk>
					</hdd1>
					<hdd2>
						<disk>hdd2</disk>
					</hdd2>
				</volumes>
				<move_factor>0.0</move_factor>
			</multiple_disks>
			<s3>
				<volumes>
					<main>
						<disk>s3</disk>
					</main>
					<external>
						<disk>default</disk>
					</external>
				</volumes>
				<move_factor>0.0</move_factor>
			</s3>
			<s3_cold>
				<volumes>
					<main>
						<disk>default</disk>
					</main>
					<external>
						<disk>s3</disk>
					</external>
				</volumes>
				<move_factor>0.0</move_factor>
			</s3_cold>
		</policies>
	</storage_configuration>
```
#### Access configuration
**query**
```sql
SHOW ACCESS
```
**result**
```
CREATE USER default IDENTIFIED WITH plaintext_password SETTINGS PROFILE `default`
CREATE SETTINGS PROFILE default SETTINGS max_memory_usage = 10000000000, load_balancing = 'random'
CREATE SETTINGS PROFILE readonly SETTINGS readonly = 1
CREATE QUOTA default KEYED BY user_name FOR INTERVAL 1 hour TRACKING ONLY TO default
GRANT ALL ON *.* TO default WITH GRANT OPTION
```
#### Quotas
**query**
```sql
SHOW QUOTA
```
**result**
```
Row 1:
──────
quota_name:         default
quota_key:          default
start_time:         2021-12-25 15:00:00
end_time:           2021-12-25 16:00:00
duration:           3600
queries:            49
max_queries:        ᴺᵁᴸᴸ
query_selects:      49
max_query_selects:  ᴺᵁᴸᴸ
query_inserts:      0
max_query_inserts:  ᴺᵁᴸᴸ
errors:             6
max_errors:         ᴺᵁᴸᴸ
result_rows:        607
max_result_rows:    ᴺᵁᴸᴸ
result_bytes:       237632
max_result_bytes:   ᴺᵁᴸᴸ
read_rows:          1256
max_read_rows:      ᴺᵁᴸᴸ
read_bytes:         778936
max_read_bytes:     ᴺᵁᴸᴸ
execution_time:     0
max_execution_time: ᴺᵁᴸᴸ
```
#### Schema
##### Database engines
**query**
```sql
SELECT
    engine,
    count() "count"
FROM system.databases
GROUP BY engine
```
**result**
```
┌─engine─┬─count─┐
│ Memory │     2 │
│ Atomic │     2 │
└────────┴───────┘
```
##### Databases (top 10 by size)
**query**
```sql
SELECT
    name,
    engine,
    tables,
    partitions,
    parts,
    formatReadableSize(bytes_on_disk) "disk_size"
FROM system.databases db
LEFT JOIN
(
    SELECT
        database,
        uniq(table) "tables",
        uniq(table, partition) "partitions",
        count() AS parts,
        sum(bytes_on_disk) "bytes_on_disk"
    FROM system.parts
    WHERE active
    GROUP BY database
) AS db_stats ON db.name = db_stats.database
ORDER BY bytes_on_disk DESC
LIMIT 10
```
**result**
```
┌─name───────────────┬─engine─┬─tables─┬─partitions─┬─parts─┬─disk_size──┐
│ system             │ Atomic │      6 │          6 │    22 │ 716.29 KiB │
│ INFORMATION_SCHEMA │ Memory │      0 │          0 │     0 │ 0.00 B     │
│ default            │ Atomic │      0 │          0 │     0 │ 0.00 B     │
│ information_schema │ Memory │      0 │          0 │     0 │ 0.00 B     │
└────────────────────┴────────┴────────┴────────────┴───────┴────────────┘
```
##### Table engines
**query**
```sql
SELECT
    engine,
    count() "count"
FROM system.tables
WHERE database != 'system'
GROUP BY engine
```
**result**
```
┌─engine─┬─count─┐
│ View   │     8 │
└────────┴───────┘
```
##### Dictionaries
**query**
```sql
SELECT
    source,
    type,
    status,
    count() "count"
FROM system.dictionaries
GROUP BY source, type, status
ORDER BY status DESC, source
```
**result**
```

```
#### Replication
##### Replicated tables (top 10 by absolute delay)
**query**
```sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    absolute_delay,
    queue_size,
    inserts_in_queue,
    merges_in_queue
FROM system.replicas
ORDER BY absolute_delay DESC
LIMIT 10
```
**result**
```

```
##### Replication queue (top 20 oldest tasks)
**query**
```sql
SELECT
    database,
    table,
    replica_name,
    position,
    node_name,
    type,
    source_replica,
    parts_to_merge,
    new_part_name,
    create_time,
    required_quorum,
    is_detach,
    is_currently_executing,
    num_tries,
    last_attempt_time,
    last_exception,
    concat('time: ', toString(last_postpone_time), ', number: ', toString(num_postponed), ', reason: ', postpone_reason) postpone
FROM system.replication_queue
ORDER BY create_time ASC
LIMIT 20
```
**result**
```

```
##### Replicated fetches
**query**
```sql
SELECT
    database,
    table,
    round(elapsed, 1) "elapsed",
    round(100 * progress, 1) "progress",
    partition_id,
    result_part_name,
    result_part_path,
    total_size_bytes_compressed,
    bytes_read_compressed,
    source_replica_path,
    source_replica_hostname,
    source_replica_port,
    interserver_scheme,
    to_detached,
    thread_id
FROM system.replicated_fetches
```
**result**
```

```
#### Top 10 tables by max parts per partition
**query**
```sql
SELECT
        database,
    table,
    count() "partitions",
    sum(part_count) "parts",
    max(part_count) "max_parts_per_partition"
FROM
(
    SELECT
        database,
        table,
        partition,
        count() "part_count"
    FROM system.parts
    WHERE active
    GROUP BY database, table, partition
) partitions
GROUP BY database, table
ORDER BY max_parts_per_partition DESC
LIMIT 10
```
**result**
```
┌─database─┬─table───────────────────┬─partitions─┬─parts─┬─max_parts_per_partition─┐
│ system   │ metric_log              │          1 │     5 │                       5 │
│ system   │ trace_log               │          1 │     5 │                       5 │
│ system   │ query_thread_log        │          1 │     3 │                       3 │
│ system   │ query_log               │          1 │     3 │                       3 │
│ system   │ asynchronous_metric_log │          1 │     3 │                       3 │
│ system   │ session_log             │          1 │     3 │                       3 │
└──────────┴─────────────────────────┴────────────┴───────┴─────────────────────────┘
```
#### Merges in progress
**query**
```sql
SELECT
    database,
    table,
    round(elapsed, 1) "elapsed",
    round(100 * progress, 1) "progress",
    is_mutation,
    partition_id,
result_part_path,
    source_part_paths,
num_parts,
    formatReadableSize(total_size_bytes_compressed) "total_size_compressed",
    formatReadableSize(bytes_read_uncompressed) "read_uncompressed",
    formatReadableSize(bytes_written_uncompressed) "written_uncompressed",
    columns_written,
formatReadableSize(memory_usage) "memory_usage",
    thread_id
FROM system.merges
```
**result**
```

```
#### Mutations in progress
**query**
```sql
SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
parts_to_do_names,
parts_to_do,
    is_done,
    latest_failed_part,
    latest_fail_time,
    latest_fail_reason
FROM system.mutations
WHERE NOT is_done
ORDER BY create_time DESC
```
**result**
```

```
#### Recent data parts (modification time within last 3 minutes)
**query**
```sql
SELECT
    database,
    table,
    engine,
    partition_id,
    name,
part_type,
active,
    level,
disk_name,
path,
    marks,
    rows,
    bytes_on_disk,
    data_compressed_bytes,
    data_uncompressed_bytes,
    marks_bytes,
    modification_time,
    remove_time,
    refcount,
    is_frozen,
    min_date,
    max_date,
    min_time,
    max_time,
    min_block_number,
    max_block_number
FROM system.parts
WHERE modification_time > now() - INTERVAL 3 MINUTE
ORDER BY modification_time DESC
```
**result**
```
Row 1:
──────
database:                system
table:                   metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_110_110_0
part_type:               Compact
active:                  1
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/9a2/9a2fb3b4-8ced-4c0b-9a2f-b3b48ced4c0b/202112_110_110_0/
marks:                   2
rows:                    8
bytes_on_disk:           21752
data_compressed_bytes:   11699
data_uncompressed_bytes: 19952
marks_bytes:             10032
modification_time:       2021-12-25 15:33:59
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        110
max_block_number:        110

Row 2:
──────
database:                system
table:                   asynchronous_metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_118_118_0
part_type:               Compact
active:                  1
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/78e/78e6eec8-3f71-4724-b8e6-eec83f71a724/202112_118_118_0/
marks:                   2
rows:                    4767
bytes_on_disk:           10856
data_compressed_bytes:   10656
data_uncompressed_bytes: 128675
marks_bytes:             176
modification_time:       2021-12-25 15:33:58
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        118
max_block_number:        118

Row 3:
──────
database:                system
table:                   asynchronous_metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_117_117_0
part_type:               Compact
active:                  1
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/78e/78e6eec8-3f71-4724-b8e6-eec83f71a724/202112_117_117_0/
marks:                   2
rows:                    4767
bytes_on_disk:           11028
data_compressed_bytes:   10828
data_uncompressed_bytes: 128675
marks_bytes:             176
modification_time:       2021-12-25 15:33:51
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        117
max_block_number:        117

Row 4:
──────
database:                system
table:                   metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_109_109_0
part_type:               Compact
active:                  1
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/9a2/9a2fb3b4-8ced-4c0b-9a2f-b3b48ced4c0b/202112_109_109_0/
marks:                   2
rows:                    7
bytes_on_disk:           21802
data_compressed_bytes:   11749
data_uncompressed_bytes: 17458
marks_bytes:             10032
modification_time:       2021-12-25 15:33:51
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        109
max_block_number:        109

Row 5:
──────
database:                system
table:                   trace_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_53_53_0
part_type:               Compact
active:                  1
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/c0b/c0bc3be3-22d7-45a3-80bc-3be322d7b5a3/202112_53_53_0/
marks:                   2
rows:                    6
bytes_on_disk:           1057
data_compressed_bytes:   700
data_uncompressed_bytes: 1894
marks_bytes:             336
modification_time:       2021-12-25 15:33:49
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        53
max_block_number:        53

Row 6:
──────
database:                system
table:                   asynchronous_metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_116_116_0
part_type:               Compact
active:                  0
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/78e/78e6eec8-3f71-4724-b8e6-eec83f71a724/202112_116_116_0/
marks:                   2
rows:                    4767
bytes_on_disk:           10911
data_compressed_bytes:   10711
data_uncompressed_bytes: 128675
marks_bytes:             176
modification_time:       2021-12-25 15:33:44
remove_time:             2021-12-25 15:33:44
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        116
max_block_number:        116

Row 7:
──────
database:                system
table:                   asynchronous_metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_1_116_23
part_type:               Wide
active:                  1
level:                   23
disk_name:               default
path:                    /var/lib/clickhouse/store/78e/78e6eec8-3f71-4724-b8e6-eec83f71a724/202112_1_116_23/
marks:                   69
rows:                    553071
bytes_on_disk:           435279
data_compressed_bytes:   424915
data_uncompressed_bytes: 13289123
marks_bytes:             9936
modification_time:       2021-12-25 15:33:44
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        1
max_block_number:        116

Row 8:
──────
database:                system
table:                   metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_108_108_0
part_type:               Compact
active:                  1
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/9a2/9a2fb3b4-8ced-4c0b-9a2f-b3b48ced4c0b/202112_108_108_0/
marks:                   2
rows:                    8
bytes_on_disk:           21833
data_compressed_bytes:   11780
data_uncompressed_bytes: 19952
marks_bytes:             10032
modification_time:       2021-12-25 15:33:44
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        108
max_block_number:        108

Row 9:
───────
database:                system
table:                   asynchronous_metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_115_115_0
part_type:               Compact
active:                  0
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/78e/78e6eec8-3f71-4724-b8e6-eec83f71a724/202112_115_115_0/
marks:                   2
rows:                    4767
bytes_on_disk:           11146
data_compressed_bytes:   10946
data_uncompressed_bytes: 128675
marks_bytes:             176
modification_time:       2021-12-25 15:33:37
remove_time:             2021-12-25 15:33:44
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        115
max_block_number:        115

Row 10:
───────
database:                system
table:                   metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_107_107_0
part_type:               Compact
active:                  1
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/9a2/9a2fb3b4-8ced-4c0b-9a2f-b3b48ced4c0b/202112_107_107_0/
marks:                   2
rows:                    7
bytes_on_disk:           21996
data_compressed_bytes:   11943
data_uncompressed_bytes: 17458
marks_bytes:             10032
modification_time:       2021-12-25 15:33:36
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        107
max_block_number:        107

Row 11:
───────
database:                system
table:                   session_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_3_3_0
part_type:               Compact
active:                  1
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/9f3/9f3dd592-781c-48d8-9f3d-d592781c48d8/202112_3_3_0/
marks:                   2
rows:                    44
bytes_on_disk:           2208
data_compressed_bytes:   1498
data_uncompressed_bytes: 5130
marks_bytes:             688
modification_time:       2021-12-25 15:33:34
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        3
max_block_number:        3

Row 12:
───────
database:                system
table:                   query_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_3_3_0
part_type:               Compact
active:                  1
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/1a3/1a3ec308-d42e-4f3c-9a3e-c308d42e2f3c/202112_3_3_0/
marks:                   2
rows:                    43
bytes_on_disk:           17843
data_compressed_bytes:   15725
data_uncompressed_bytes: 61869
marks_bytes:             2096
modification_time:       2021-12-25 15:33:34
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        3
max_block_number:        3

Row 13:
───────
database:                system
table:                   query_thread_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_3_3_0
part_type:               Compact
active:                  1
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/afa/afa652ef-f91d-4a48-afa6-52eff91daa48/202112_3_3_0/
marks:                   2
rows:                    43
bytes_on_disk:           11878
data_compressed_bytes:   10432
data_uncompressed_bytes: 52339
marks_bytes:             1424
modification_time:       2021-12-25 15:33:34
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        3
max_block_number:        3

Row 14:
───────
database:                system
table:                   trace_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_52_52_0
part_type:               Compact
active:                  1
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/c0b/c0bc3be3-22d7-45a3-80bc-3be322d7b5a3/202112_52_52_0/
marks:                   2
rows:                    4
bytes_on_disk:           1078
data_compressed_bytes:   721
data_uncompressed_bytes: 1252
marks_bytes:             336
modification_time:       2021-12-25 15:33:34
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        52
max_block_number:        52

Row 15:
───────
database:                system
table:                   asynchronous_metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_114_114_0
part_type:               Compact
active:                  0
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/78e/78e6eec8-3f71-4724-b8e6-eec83f71a724/202112_114_114_0/
marks:                   2
rows:                    4767
bytes_on_disk:           11447
data_compressed_bytes:   11247
data_uncompressed_bytes: 128675
marks_bytes:             176
modification_time:       2021-12-25 15:33:30
remove_time:             2021-12-25 15:33:44
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        114
max_block_number:        114

Row 16:
───────
database:                system
table:                   metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_1_106_21
part_type:               Compact
active:                  1
level:                   21
disk_name:               default
path:                    /var/lib/clickhouse/store/9a2/9a2fb3b4-8ced-4c0b-9a2f-b3b48ced4c0b/202112_1_106_21/
marks:                   2
rows:                    798
bytes_on_disk:           84853
data_compressed_bytes:   74798
data_uncompressed_bytes: 1990212
marks_bytes:             10032
modification_time:       2021-12-25 15:33:29
remove_time:             1970-01-01 03:00:00
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        1
max_block_number:        106

Row 17:
───────
database:                system
table:                   metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_106_106_0
part_type:               Compact
active:                  0
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/9a2/9a2fb3b4-8ced-4c0b-9a2f-b3b48ced4c0b/202112_106_106_0/
marks:                   2
rows:                    8
bytes_on_disk:           21863
data_compressed_bytes:   11810
data_uncompressed_bytes: 19952
marks_bytes:             10032
modification_time:       2021-12-25 15:33:28
remove_time:             2021-12-25 15:33:29
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        106
max_block_number:        106

Row 18:
───────
database:                system
table:                   asynchronous_metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_113_113_0
part_type:               Compact
active:                  0
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/78e/78e6eec8-3f71-4724-b8e6-eec83f71a724/202112_113_113_0/
marks:                   2
rows:                    4767
bytes_on_disk:           11191
data_compressed_bytes:   10991
data_uncompressed_bytes: 128675
marks_bytes:             176
modification_time:       2021-12-25 15:33:23
remove_time:             2021-12-25 15:33:44
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        113
max_block_number:        113

Row 19:
───────
database:                system
table:                   metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_105_105_0
part_type:               Compact
active:                  0
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/9a2/9a2fb3b4-8ced-4c0b-9a2f-b3b48ced4c0b/202112_105_105_0/
marks:                   2
rows:                    7
bytes_on_disk:           21786
data_compressed_bytes:   11733
data_uncompressed_bytes: 17458
marks_bytes:             10032
modification_time:       2021-12-25 15:33:21
remove_time:             2021-12-25 15:33:29
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        105
max_block_number:        105

Row 20:
───────
database:                system
table:                   asynchronous_metric_log
engine:                  MergeTree
partition_id:            202112
name:                    202112_112_112_0
part_type:               Compact
active:                  0
level:                   0
disk_name:               default
path:                    /var/lib/clickhouse/store/78e/78e6eec8-3f71-4724-b8e6-eec83f71a724/202112_112_112_0/
marks:                   2
rows:                    4767
bytes_on_disk:           11281
data_compressed_bytes:   11081
data_uncompressed_bytes: 128675
marks_bytes:             176
modification_time:       2021-12-25 15:33:16
remove_time:             2021-12-25 15:33:44
refcount:                1
is_frozen:               0
min_date:                2021-12-25
max_date:                2021-12-25
min_time:                1970-01-01 03:00:00
max_time:                1970-01-01 03:00:00
min_block_number:        112
max_block_number:        112
```
#### Detached data
##### system.detached_parts
**query**
```sql
SELECT
    database,
    table,
    partition_id,
    name,
    disk,
    reason,
    min_block_number,
    max_block_number,
    level
FROM system.detached_parts
```
**result**
```
┌─database─┬─table─┬─partition_id─┬─name─┬─disk─┬─reason─┬─min_block_number─┬─max_block_number─┬─level─┐
└──────────┴───────┴──────────────┴──────┴──────┴────────┴──────────────────┴──────────────────┴───────┘
```
##### Disk space usage
**command**
```
du -sh -L -c /var/lib/clickhouse/data/*/*/detached/* | sort -rsh
```
**result**
```
0	total

```
#### Queries
##### Queries in progress (process list)
**query**
```sql
SELECT
    elapsed,
    query_id,
    query,
    is_cancelled,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    formatReadableSize(memory_usage) AS "memory usage",
    user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    thread_ids,
    ProfileEvents,
    Settings
    FROM system.processes
ORDER BY elapsed DESC
```
**result**
```
Row 1:
──────
elapsed:       0.000785246
query_id:      b51cbc7a-2260-4c9b-b26c-6307b10ad948
query:         SELECT
    elapsed,
    query_id,
    query,
    is_cancelled,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    formatReadableSize(memory_usage) AS "memory usage",
    user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    thread_ids,
    ProfileEvents,
    Settings
    FROM system.processes
ORDER BY elapsed DESC FORMAT Vertical

is_cancelled:  0
read:          0 rows / 0.00 B
written:       0 rows / 0.00 B
memory usage:  0.00 B
user:          default
client:        python-requests/2.26.0
thread_ids:    [66]
ProfileEvents: {'Query':1,'SelectQuery':1,'ContextLock':38,'RWLockAcquiredReadLocks':1}
Settings:      {'load_balancing':'random','max_memory_usage':'10000000000'}
```
##### Top 10 queries by duration
**query**
```sql
SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    query_kind,
    is_initial_query,
    query,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) AS result,
    formatReadableSize(memory_usage) AS "memory usage",
    exception,
    '\n' || stack_trace AS stack_trace,
    user,
    initial_user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    client_hostname,
    databases,
    tables,
    columns,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_database_engines,
    used_data_type_families,
    used_dictionaries,
    used_formats,
    used_functions,
    used_storages,
    used_table_functions,
    thread_ids,
    ProfileEvents,
    Settings
    FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
ORDER BY query_duration_ms DESC
LIMIT 10
```
**result**
```
Row 1:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:25:01
query_duration_ms:                   60
query_id:                            f72e1120-cc66-434c-9809-3a99077ed842
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT
        database,
    table,
    count() "partitions",
    sum(part_count) "parts",
    max(part_count) "max_parts_per_partition"
FROM
(
    SELECT
        database,
        table,
        partition,
        count() "part_count"
    FROM system.parts
    WHERE active
    GROUP BY database, table, partition
) partitions
GROUP BY database, table
ORDER BY max_parts_per_partition DESC
LIMIT 10 FORMAT PrettyCompactNoEscapes

read:                                5 rows / 262.00 B
written:                             0 rows / 0.00 B
result:                              3 rows / 488.00 B
memory usage:                        0.00 B
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.parts']
columns:                             ['system.parts.active','system.parts.database','system.parts.partition','system.parts.table']
used_aggregate_functions:            ['count','max','sum']
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['PrettyCompactNoEscapes']
used_functions:                      []
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'ArenaAllocChunks':2,'ArenaAllocBytes':8192,'CompileFunction':1,'CompileExpressionsMicroseconds':52574,'CompileExpressionsBytes':8192,'SelectedRows':5,'SelectedBytes':262,'ContextLock':58,'RWLockAcquiredReadLocks':6,'RealTimeMicroseconds':61493,'UserTimeMicroseconds':34154,'SystemTimeMicroseconds':9874,'SoftPageFaults':170,'HardPageFaults':33,'OSIOWaitMicroseconds':10000,'OSCPUWaitMicroseconds':2433,'OSCPUVirtualTimeMicroseconds':43706,'OSReadBytes':3080192,'OSWriteBytes':4096,'OSReadChars':863,'OSWriteChars':5334}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 2:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:26:26
query_duration_ms:                   12
query_id:                            eabd7483-70df-4d60-a668-d8961416e3fb
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    query_kind,
    is_initial_query,
    query,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) AS result,
    formatReadableSize(memory_usage) AS "memory usage",
    exception,
    '\n' || stack_trace AS stack_trace,
    user,
    initial_user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    client_hostname,
    databases,
    tables,
    columns,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_database_engines,
    used_data_type_families,
    used_dictionaries,
    used_formats,
    used_functions,
    used_storages,
    used_table_functions,
    thread_ids,
    ProfileEvents,
    Settings
    FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
ORDER BY query_duration_ms DESC
LIMIT 10 FORMAT Vertical

read:                                40 rows / 67.42 KiB
written:                             0 rows / 0.00 B
result:                              10 rows / 41.23 KiB
memory usage:                        0.00 B
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.query_log']
columns:                             ['system.query_log.ProfileEvents','system.query_log.Settings','system.query_log.client_hostname','system.query_log.client_name','system.query_log.client_version_major','system.query_log.client_version_minor','system.query_log.client_version_patch','system.query_log.columns','system.query_log.databases','system.query_log.event_date','system.query_log.event_time','system.query_log.exception','system.query_log.http_user_agent','system.query_log.initial_user','system.query_log.is_initial_query','system.query_log.memory_usage','system.query_log.query','system.query_log.query_duration_ms','system.query_log.query_id','system.query_log.query_kind','system.query_log.query_start_time','system.query_log.read_bytes','system.query_log.read_rows','system.query_log.result_bytes','system.query_log.result_rows','system.query_log.stack_trace','system.query_log.tables','system.query_log.thread_ids','system.query_log.type','system.query_log.used_aggregate_function_combinators','system.query_log.used_aggregate_functions','system.query_log.used_data_type_families','system.query_log.used_database_engines','system.query_log.used_dictionaries','system.query_log.used_formats','system.query_log.used_functions','system.query_log.used_storages','system.query_log.used_table_functions','system.query_log.user','system.query_log.written_bytes','system.query_log.written_rows']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['Vertical']
used_functions:                      ['empty','and','now','concat','today','toIntervalDay','formatReadableSize','minus','greaterOrEquals','multiIf','toString','subtractDays','notEquals']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'FileOpen':2,'Seek':3,'ReadBufferFromFileDescriptorRead':10,'ReadBufferFromFileDescriptorReadBytes':16873,'ReadCompressedBytes':12855,'CompressedReadBufferBlocks':41,'CompressedReadBufferBytes':61376,'IOBufferAllocs':5,'IOBufferAllocBytes':26594,'FunctionExecute':28,'MarkCacheHits':1,'MarkCacheMisses':1,'CreatedReadBufferOrdinary':3,'DiskReadElapsedMicroseconds':30,'SelectedParts':1,'SelectedRanges':1,'SelectedMarks':1,'SelectedRows':40,'SelectedBytes':69039,'ContextLock':342,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':14451,'UserTimeMicroseconds':10009,'SystemTimeMicroseconds':1515,'SoftPageFaults':44,'OSCPUWaitMicroseconds':3050,'OSCPUVirtualTimeMicroseconds':11523,'OSReadChars':17311,'OSWriteChars':7288}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 3:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:33:29
query_duration_ms:                   12
query_id:                            d9557845-5b5e-44ef-befa-55f837065d00
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    query_kind,
    is_initial_query,
    query,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) AS result,
    formatReadableSize(memory_usage) AS "memory usage",
    exception,
    '\n' || stack_trace AS stack_trace,
    user,
    initial_user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    client_hostname,
    databases,
    tables,
    columns,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_database_engines,
    used_data_type_families,
    used_dictionaries,
    used_formats,
    used_functions,
    used_storages,
    used_table_functions,
    thread_ids,
    ProfileEvents,
    Settings
    FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
ORDER BY query_duration_ms DESC
LIMIT 10 FORMAT Vertical

read:                                83 rows / 130.00 KiB
written:                             0 rows / 0.00 B
result:                              10 rows / 183.10 KiB
memory usage:                        0.00 B
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.query_log']
columns:                             ['system.query_log.ProfileEvents','system.query_log.Settings','system.query_log.client_hostname','system.query_log.client_name','system.query_log.client_version_major','system.query_log.client_version_minor','system.query_log.client_version_patch','system.query_log.columns','system.query_log.databases','system.query_log.event_date','system.query_log.event_time','system.query_log.exception','system.query_log.http_user_agent','system.query_log.initial_user','system.query_log.is_initial_query','system.query_log.memory_usage','system.query_log.query','system.query_log.query_duration_ms','system.query_log.query_id','system.query_log.query_kind','system.query_log.query_start_time','system.query_log.read_bytes','system.query_log.read_rows','system.query_log.result_bytes','system.query_log.result_rows','system.query_log.stack_trace','system.query_log.tables','system.query_log.thread_ids','system.query_log.type','system.query_log.used_aggregate_function_combinators','system.query_log.used_aggregate_functions','system.query_log.used_data_type_families','system.query_log.used_database_engines','system.query_log.used_dictionaries','system.query_log.used_formats','system.query_log.used_functions','system.query_log.used_storages','system.query_log.used_table_functions','system.query_log.user','system.query_log.written_bytes','system.query_log.written_rows']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['Vertical']
used_functions:                      ['empty','and','now','concat','today','toIntervalDay','formatReadableSize','minus','greaterOrEquals','multiIf','toString','subtractDays','notEquals']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66,283,225,281,282]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'FileOpen':3,'Seek':6,'ReadBufferFromFileDescriptorRead':18,'ReadBufferFromFileDescriptorReadBytes':32140,'ReadCompressedBytes':25892,'CompressedReadBufferBlocks':82,'CompressedReadBufferBytes':116215,'IOBufferAllocs':9,'IOBufferAllocBytes':47368,'FunctionExecute':51,'MarkCacheHits':3,'MarkCacheMisses':1,'CreatedReadBufferOrdinary':5,'DiskReadElapsedMicroseconds':13,'SelectedParts':2,'SelectedRanges':2,'SelectedMarks':2,'SelectedRows':83,'SelectedBytes':133125,'ContextLock':351,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':19368,'UserTimeMicroseconds':12036,'SystemTimeMicroseconds':2047,'SoftPageFaults':42,'OSCPUWaitMicroseconds':710,'OSCPUVirtualTimeMicroseconds':13623,'OSWriteBytes':4096,'OSReadChars':34225,'OSWriteChars':8142}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 4:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:33:29
query_duration_ms:                   11
query_id:                            bae8a338-eee9-406b-80d2-4596af2ba31f
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT
    name,
    engine,
    tables,
    partitions,
    parts,
    formatReadableSize(bytes_on_disk) "disk_size"
FROM system.databases db
LEFT JOIN
(
    SELECT
        database,
        uniq(table) "tables",
        uniq(table, partition) "partitions",
        count() AS parts,
        sum(bytes_on_disk) "bytes_on_disk"
    FROM system.parts
    WHERE active
    GROUP BY database
) AS db_stats ON db.name = db_stats.database
ORDER BY bytes_on_disk DESC
LIMIT 10 FORMAT PrettyCompactNoEscapes

read:                                17 rows / 1.31 KiB
written:                             0 rows / 0.00 B
result:                              4 rows / 640.00 B
memory usage:                        0.00 B
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.databases','system.parts']
columns:                             ['system.databases.engine','system.databases.name','system.parts.active','system.parts.bytes_on_disk','system.parts.database','system.parts.partition','system.parts.table']
used_aggregate_functions:            ['count','sum','uniq']
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['PrettyCompactNoEscapes']
used_functions:                      ['formatReadableSize']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'ArenaAllocChunks':5,'ArenaAllocBytes':20480,'FunctionExecute':1,'SelectedRows':17,'SelectedBytes':1345,'ContextLock':69,'RWLockAcquiredReadLocks':9,'RealTimeMicroseconds':12225,'UserTimeMicroseconds':10731,'SystemTimeMicroseconds':1146,'SoftPageFaults':2,'OSCPUWaitMicroseconds':720,'OSCPUVirtualTimeMicroseconds':11876,'OSWriteBytes':4096,'OSReadChars':438,'OSWriteChars':8938}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 5:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:26:26
query_duration_ms:                   9
query_id:                            f0c62bc7-36da-4542-a3d5-68a40c1c4b48
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    query_kind,
    is_initial_query,
    query,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) AS result,
    formatReadableSize(memory_usage) AS "memory usage",
    exception,
    '\n' || stack_trace AS stack_trace,
    user,
    initial_user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    client_hostname,
    databases,
    tables,
    columns,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_database_engines,
    used_data_type_families,
    used_dictionaries,
    used_formats,
    used_functions,
    used_storages,
    used_table_functions,
    thread_ids,
    ProfileEvents,
    Settings
    FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
  AND exception != ''
ORDER BY query_start_time DESC
LIMIT 10 FORMAT Vertical

read:                                40 rows / 67.42 KiB
written:                             0 rows / 0.00 B
result:                              4 rows / 43.13 KiB
memory usage:                        0.00 B
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.query_log']
columns:                             ['system.query_log.ProfileEvents','system.query_log.Settings','system.query_log.client_hostname','system.query_log.client_name','system.query_log.client_version_major','system.query_log.client_version_minor','system.query_log.client_version_patch','system.query_log.columns','system.query_log.databases','system.query_log.event_date','system.query_log.event_time','system.query_log.exception','system.query_log.http_user_agent','system.query_log.initial_user','system.query_log.is_initial_query','system.query_log.memory_usage','system.query_log.query','system.query_log.query_duration_ms','system.query_log.query_id','system.query_log.query_kind','system.query_log.query_start_time','system.query_log.read_bytes','system.query_log.read_rows','system.query_log.result_bytes','system.query_log.result_rows','system.query_log.stack_trace','system.query_log.tables','system.query_log.thread_ids','system.query_log.type','system.query_log.used_aggregate_function_combinators','system.query_log.used_aggregate_functions','system.query_log.used_data_type_families','system.query_log.used_database_engines','system.query_log.used_dictionaries','system.query_log.used_formats','system.query_log.used_functions','system.query_log.used_storages','system.query_log.used_table_functions','system.query_log.user','system.query_log.written_bytes','system.query_log.written_rows']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['Vertical']
used_functions:                      ['empty','and','now','concat','today','toIntervalDay','formatReadableSize','minus','greaterOrEquals','multiIf','toString','subtractDays','notEquals']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'FileOpen':1,'Seek':3,'ReadBufferFromFileDescriptorRead':8,'ReadBufferFromFileDescriptorReadBytes':15561,'ReadCompressedBytes':12855,'CompressedReadBufferBlocks':41,'CompressedReadBufferBytes':61376,'IOBufferAllocs':4,'IOBufferAllocBytes':25506,'FunctionExecute':31,'MarkCacheHits':2,'CreatedReadBufferOrdinary':2,'DiskReadElapsedMicroseconds':16,'SelectedParts':1,'SelectedRanges':1,'SelectedMarks':1,'SelectedRows':40,'SelectedBytes':69039,'ContextLock':361,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':11353,'UserTimeMicroseconds':8910,'SystemTimeMicroseconds':533,'SoftPageFaults':7,'HardPageFaults':2,'OSCPUWaitMicroseconds':1117,'OSCPUVirtualTimeMicroseconds':9443,'OSReadBytes':16384,'OSWriteBytes':4096,'OSReadChars':15999,'OSWriteChars':7714,'QueryProfilerRuns':1}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 6:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:33:29
query_duration_ms:                   8
query_id:                            72f3f9de-d17c-456b-8316-d494bea2096a
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT name FROM system.tables WHERE database = 'system' FORMAT JSONCompact

read:                                74 rows / 2.61 KiB
written:                             0 rows / 0.00 B
result:                              74 rows / 2.00 KiB
memory usage:                        0.00 B
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.tables']
columns:                             ['system.tables.database','system.tables.name']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['JSONCompact']
used_functions:                      ['equals']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'IOBufferAllocs':2,'IOBufferAllocBytes':8192,'FunctionExecute':4,'SelectedRows':74,'SelectedBytes':2675,'ContextLock':23,'RWLockAcquiredReadLocks':75,'RealTimeMicroseconds':9190,'UserTimeMicroseconds':6468,'SystemTimeMicroseconds':517,'OSCPUWaitMicroseconds':2237,'OSCPUVirtualTimeMicroseconds':6984,'OSReadChars':438,'OSWriteChars':1270}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 7:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:33:29
query_duration_ms:                   8
query_id:                            d55da87f-b030-4b5d-95fc-f9103ce58601
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    query_kind,
    is_initial_query,
    query,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) AS result,
    formatReadableSize(memory_usage) AS "memory usage",
    exception,
    '\n' || stack_trace AS stack_trace,
    user,
    initial_user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    client_hostname,
    databases,
    tables,
    columns,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_database_engines,
    used_data_type_families,
    used_dictionaries,
    used_formats,
    used_functions,
    used_storages,
    used_table_functions,
    thread_ids,
    ProfileEvents,
    Settings
    FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
ORDER BY memory_usage DESC
LIMIT 10 FORMAT Vertical

read:                                83 rows / 130.00 KiB
written:                             0 rows / 0.00 B
result:                              10 rows / 178.41 KiB
memory usage:                        0.00 B
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.query_log']
columns:                             ['system.query_log.ProfileEvents','system.query_log.Settings','system.query_log.client_hostname','system.query_log.client_name','system.query_log.client_version_major','system.query_log.client_version_minor','system.query_log.client_version_patch','system.query_log.columns','system.query_log.databases','system.query_log.event_date','system.query_log.event_time','system.query_log.exception','system.query_log.http_user_agent','system.query_log.initial_user','system.query_log.is_initial_query','system.query_log.memory_usage','system.query_log.query','system.query_log.query_duration_ms','system.query_log.query_id','system.query_log.query_kind','system.query_log.query_start_time','system.query_log.read_bytes','system.query_log.read_rows','system.query_log.result_bytes','system.query_log.result_rows','system.query_log.stack_trace','system.query_log.tables','system.query_log.thread_ids','system.query_log.type','system.query_log.used_aggregate_function_combinators','system.query_log.used_aggregate_functions','system.query_log.used_data_type_families','system.query_log.used_database_engines','system.query_log.used_dictionaries','system.query_log.used_formats','system.query_log.used_functions','system.query_log.used_storages','system.query_log.used_table_functions','system.query_log.user','system.query_log.written_bytes','system.query_log.written_rows']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['Vertical']
used_functions:                      ['empty','and','now','concat','today','toIntervalDay','formatReadableSize','minus','greaterOrEquals','multiIf','toString','subtractDays','notEquals']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66,283,283,225,282]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'FileOpen':2,'Seek':6,'ReadBufferFromFileDescriptorRead':16,'ReadBufferFromFileDescriptorReadBytes':30044,'ReadCompressedBytes':25892,'CompressedReadBufferBlocks':82,'CompressedReadBufferBytes':116215,'IOBufferAllocs':8,'IOBufferAllocBytes':45272,'FunctionExecute':51,'MarkCacheHits':4,'CreatedReadBufferOrdinary':4,'SelectedParts':2,'SelectedRanges':2,'SelectedMarks':2,'SelectedRows':83,'SelectedBytes':133125,'ContextLock':351,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':12416,'UserTimeMicroseconds':7727,'SystemTimeMicroseconds':1247,'SoftPageFaults':41,'OSCPUWaitMicroseconds':1058,'OSCPUVirtualTimeMicroseconds':9018,'OSWriteBytes':4096,'OSReadChars':32137,'OSWriteChars':8108}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 8:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:33:29
query_duration_ms:                   8
query_id:                            cc2a0e7a-3b9b-47d2-9255-009c62584bc4
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    query_kind,
    is_initial_query,
    query,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) AS result,
    formatReadableSize(memory_usage) AS "memory usage",
    exception,
    '\n' || stack_trace AS stack_trace,
    user,
    initial_user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    client_hostname,
    databases,
    tables,
    columns,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_database_engines,
    used_data_type_families,
    used_dictionaries,
    used_formats,
    used_functions,
    used_storages,
    used_table_functions,
    thread_ids,
    ProfileEvents,
    Settings
    FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
  AND exception != ''
ORDER BY query_start_time DESC
LIMIT 10 FORMAT Vertical

read:                                83 rows / 130.00 KiB
written:                             0 rows / 0.00 B
result:                              5 rows / 57.80 KiB
memory usage:                        0.00 B
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.query_log']
columns:                             ['system.query_log.ProfileEvents','system.query_log.Settings','system.query_log.client_hostname','system.query_log.client_name','system.query_log.client_version_major','system.query_log.client_version_minor','system.query_log.client_version_patch','system.query_log.columns','system.query_log.databases','system.query_log.event_date','system.query_log.event_time','system.query_log.exception','system.query_log.http_user_agent','system.query_log.initial_user','system.query_log.is_initial_query','system.query_log.memory_usage','system.query_log.query','system.query_log.query_duration_ms','system.query_log.query_id','system.query_log.query_kind','system.query_log.query_start_time','system.query_log.read_bytes','system.query_log.read_rows','system.query_log.result_bytes','system.query_log.result_rows','system.query_log.stack_trace','system.query_log.tables','system.query_log.thread_ids','system.query_log.type','system.query_log.used_aggregate_function_combinators','system.query_log.used_aggregate_functions','system.query_log.used_data_type_families','system.query_log.used_database_engines','system.query_log.used_dictionaries','system.query_log.used_formats','system.query_log.used_functions','system.query_log.used_storages','system.query_log.used_table_functions','system.query_log.user','system.query_log.written_bytes','system.query_log.written_rows']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['Vertical']
used_functions:                      ['empty','and','now','concat','today','toIntervalDay','formatReadableSize','minus','greaterOrEquals','multiIf','toString','subtractDays','notEquals']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66,281,283,282,225]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'FileOpen':2,'Seek':6,'ReadBufferFromFileDescriptorRead':16,'ReadBufferFromFileDescriptorReadBytes':31464,'ReadCompressedBytes':25892,'CompressedReadBufferBlocks':82,'CompressedReadBufferBytes':116215,'IOBufferAllocs':8,'IOBufferAllocBytes':46860,'FunctionExecute':56,'MarkCacheHits':4,'CreatedReadBufferOrdinary':4,'SelectedParts':2,'SelectedRanges':2,'SelectedMarks':2,'SelectedRows':83,'SelectedBytes':133125,'ContextLock':370,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':13096,'UserTimeMicroseconds':9503,'SystemTimeMicroseconds':195,'SoftPageFaults':23,'OSCPUWaitMicroseconds':1380,'OSCPUVirtualTimeMicroseconds':9661,'OSWriteBytes':4096,'OSReadChars':33567,'OSWriteChars':8310}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 9:
───────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:25:01
query_duration_ms:                   8
query_id:                            a3d717fd-c43f-4723-a18d-557c733299f6
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT
    name,
    engine,
    tables,
    partitions,
    parts,
    formatReadableSize(bytes_on_disk) "disk_size"
FROM system.databases db
LEFT JOIN
(
    SELECT
        database,
        uniq(table) "tables",
        uniq(table, partition) "partitions",
        count() AS parts,
        sum(bytes_on_disk) "bytes_on_disk"
    FROM system.parts
    WHERE active
    GROUP BY database
) AS db_stats ON db.name = db_stats.database
ORDER BY bytes_on_disk DESC
LIMIT 10 FORMAT PrettyCompactNoEscapes

read:                                9 rows / 845.00 B
written:                             0 rows / 0.00 B
result:                              4 rows / 640.00 B
memory usage:                        0.00 B
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.databases','system.parts']
columns:                             ['system.databases.engine','system.databases.name','system.parts.active','system.parts.bytes_on_disk','system.parts.database','system.parts.partition','system.parts.table']
used_aggregate_functions:            ['count','sum','uniq']
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['PrettyCompactNoEscapes']
used_functions:                      ['formatReadableSize']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'ArenaAllocChunks':5,'ArenaAllocBytes':20480,'FunctionExecute':1,'SelectedRows':9,'SelectedBytes':845,'ContextLock':69,'RWLockAcquiredReadLocks':6,'RealTimeMicroseconds':9090,'UserTimeMicroseconds':4654,'SystemTimeMicroseconds':1171,'SoftPageFaults':8,'HardPageFaults':2,'OSCPUWaitMicroseconds':2126,'OSCPUVirtualTimeMicroseconds':5824,'OSReadBytes':212992,'OSWriteBytes':4096,'OSReadChars':427,'OSWriteChars':8936}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 10:
───────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:26:26
query_duration_ms:                   7
query_id:                            49305759-0f08-4d5a-81d8-c1a11cfc0eb4
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    query_kind,
    is_initial_query,
    query,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) AS result,
    formatReadableSize(memory_usage) AS "memory usage",
    exception,
    '\n' || stack_trace AS stack_trace,
    user,
    initial_user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    client_hostname,
    databases,
    tables,
    columns,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_database_engines,
    used_data_type_families,
    used_dictionaries,
    used_formats,
    used_functions,
    used_storages,
    used_table_functions,
    thread_ids,
    ProfileEvents,
    Settings
    FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
ORDER BY memory_usage DESC
LIMIT 10 FORMAT Vertical

read:                                40 rows / 67.42 KiB
written:                             0 rows / 0.00 B
result:                              10 rows / 57.95 KiB
memory usage:                        0.00 B
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.query_log']
columns:                             ['system.query_log.ProfileEvents','system.query_log.Settings','system.query_log.client_hostname','system.query_log.client_name','system.query_log.client_version_major','system.query_log.client_version_minor','system.query_log.client_version_patch','system.query_log.columns','system.query_log.databases','system.query_log.event_date','system.query_log.event_time','system.query_log.exception','system.query_log.http_user_agent','system.query_log.initial_user','system.query_log.is_initial_query','system.query_log.memory_usage','system.query_log.query','system.query_log.query_duration_ms','system.query_log.query_id','system.query_log.query_kind','system.query_log.query_start_time','system.query_log.read_bytes','system.query_log.read_rows','system.query_log.result_bytes','system.query_log.result_rows','system.query_log.stack_trace','system.query_log.tables','system.query_log.thread_ids','system.query_log.type','system.query_log.used_aggregate_function_combinators','system.query_log.used_aggregate_functions','system.query_log.used_data_type_families','system.query_log.used_database_engines','system.query_log.used_dictionaries','system.query_log.used_formats','system.query_log.used_functions','system.query_log.used_storages','system.query_log.used_table_functions','system.query_log.user','system.query_log.written_bytes','system.query_log.written_rows']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['Vertical']
used_functions:                      ['empty','and','now','concat','today','toIntervalDay','formatReadableSize','minus','greaterOrEquals','multiIf','toString','subtractDays','notEquals']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'FileOpen':1,'Seek':3,'ReadBufferFromFileDescriptorRead':8,'ReadBufferFromFileDescriptorReadBytes':14777,'ReadCompressedBytes':12855,'CompressedReadBufferBlocks':41,'CompressedReadBufferBytes':61376,'IOBufferAllocs':4,'IOBufferAllocBytes':24498,'FunctionExecute':28,'MarkCacheHits':2,'CreatedReadBufferOrdinary':2,'DiskReadElapsedMicroseconds':16,'SelectedParts':1,'SelectedRanges':1,'SelectedMarks':1,'SelectedRows':40,'SelectedBytes':69039,'ContextLock':342,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':9159,'UserTimeMicroseconds':4713,'SystemTimeMicroseconds':1942,'SoftPageFaults':19,'OSCPUWaitMicroseconds':2421,'OSCPUVirtualTimeMicroseconds':6655,'OSWriteBytes':4096,'OSReadChars':15215,'OSWriteChars':7278}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}
```
##### Top 10 queries by memory usage
**query**
```sql
SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    query_kind,
    is_initial_query,
    query,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) AS result,
    formatReadableSize(memory_usage) AS "memory usage",
    exception,
    '\n' || stack_trace AS stack_trace,
    user,
    initial_user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    client_hostname,
    databases,
    tables,
    columns,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_database_engines,
    used_data_type_families,
    used_dictionaries,
    used_formats,
    used_functions,
    used_storages,
    used_table_functions,
    thread_ids,
    ProfileEvents,
    Settings
    FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
ORDER BY memory_usage DESC
LIMIT 10
```
**result**
```
Row 1:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:26:25
query_duration_ms:                   0
query_id:                            c6b6a96c-d5c5-4406-98cd-80857a8412d4
query_kind:
is_initial_query:                    1
query:                               SHOW ACCESS FORMAT TSVRaw

read:                                5 rows / 405.00 B
written:                             0 rows / 0.00 B
result:                              5 rows / 4.50 KiB
memory usage:                        1.82 KiB
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           []
tables:                              []
columns:                             []
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['TSVRaw']
used_functions:                      []
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66,283,225,281]
ProfileEvents:                       {'Query':1,'IOBufferAllocs':3,'IOBufferAllocBytes':3145728,'SelectedRows':5,'SelectedBytes':405,'ContextLock':8,'RealTimeMicroseconds':959,'UserTimeMicroseconds':452,'SystemTimeMicroseconds':238,'OSCPUWaitMicroseconds':90,'OSCPUVirtualTimeMicroseconds':690,'OSWriteBytes':4096,'OSReadChars':846,'OSWriteChars':880}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 2:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:33:29
query_duration_ms:                   2
query_id:                            253362ba-40a1-4593-a4cc-30d3dfdfe0ab
query_kind:
is_initial_query:                    1
query:                               SHOW ACCESS FORMAT TSVRaw

read:                                5 rows / 405.00 B
written:                             0 rows / 0.00 B
result:                              5 rows / 4.50 KiB
memory usage:                        1.82 KiB
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           []
tables:                              []
columns:                             []
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['TSVRaw']
used_functions:                      []
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66,225,283,282]
ProfileEvents:                       {'Query':1,'IOBufferAllocs':3,'IOBufferAllocBytes':3145728,'SelectedRows':5,'SelectedBytes':405,'ContextLock':8,'RealTimeMicroseconds':4687,'UserTimeMicroseconds':2171,'SystemTimeMicroseconds':1264,'OSCPUWaitMicroseconds':513,'OSCPUVirtualTimeMicroseconds':3335,'OSReadChars':848,'OSWriteChars':880}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 3:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:25:01
query_duration_ms:                   1
query_id:                            61b20c8c-ca63-4384-adb4-ce7765d77389
query_kind:
is_initial_query:                    1
query:                               SHOW ACCESS FORMAT TSVRaw

read:                                5 rows / 405.00 B
written:                             0 rows / 0.00 B
result:                              5 rows / 4.50 KiB
memory usage:                        1.82 KiB
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           []
tables:                              []
columns:                             []
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['TSVRaw']
used_functions:                      []
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66,225,281,283]
ProfileEvents:                       {'Query':1,'IOBufferAllocs':3,'IOBufferAllocBytes':3145728,'SelectedRows':5,'SelectedBytes':405,'ContextLock':8,'RealTimeMicroseconds':3442,'UserTimeMicroseconds':715,'SystemTimeMicroseconds':485,'SoftPageFaults':1,'OSCPUWaitMicroseconds':443,'OSCPUVirtualTimeMicroseconds':1170,'OSReadChars':833,'OSWriteChars':880}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 4:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:26:25
query_duration_ms:                   1
query_id:                            13ebdab7-e368-4f9f-b47e-023dbd9e91ce
query_kind:                          Select
is_initial_query:                    1
query:
SELECT formatReadableTimeDelta(uptime())


read:                                1 rows / 1.00 B
written:                             0 rows / 0.00 B
result:                              1 rows / 128.00 B
memory usage:                        1.49 KiB
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.one']
columns:                             ['system.one.dummy']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['TabSeparated']
used_functions:                      ['uptime','formatReadableTimeDelta']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66,283,282,225,281]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'IOBufferAllocs':3,'IOBufferAllocBytes':3145728,'SelectedRows':1,'SelectedBytes':1,'ContextLock':17,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':1613,'UserTimeMicroseconds':708,'SystemTimeMicroseconds':274,'SoftPageFaults':3,'OSCPUWaitMicroseconds':2,'OSCPUVirtualTimeMicroseconds':980,'OSReadChars':846,'OSWriteChars':1190}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 5:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:33:29
query_duration_ms:                   2
query_id:                            ff330183-854b-46bc-a548-30e12a7bee9c
query_kind:                          Select
is_initial_query:                    1
query:
SELECT formatReadableTimeDelta(uptime())


read:                                1 rows / 1.00 B
written:                             0 rows / 0.00 B
result:                              1 rows / 128.00 B
memory usage:                        1.49 KiB
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.one']
columns:                             ['system.one.dummy']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['TabSeparated']
used_functions:                      ['formatReadableTimeDelta','uptime']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66,225,283,281,282]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'IOBufferAllocs':3,'IOBufferAllocBytes':3145728,'SelectedRows':1,'SelectedBytes':1,'ContextLock':17,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':4372,'UserTimeMicroseconds':1022,'SystemTimeMicroseconds':177,'OSCPUWaitMicroseconds':2070,'OSCPUVirtualTimeMicroseconds':1198,'OSWriteBytes':4096,'OSReadChars':848,'OSWriteChars':1190}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 6:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:25:01
query_duration_ms:                   3
query_id:                            b763c2f9-6234-47f7-8b30-43d619909289
query_kind:                          Select
is_initial_query:                    1
query:
SELECT formatReadableTimeDelta(uptime())


read:                                1 rows / 1.00 B
written:                             0 rows / 0.00 B
result:                              1 rows / 128.00 B
memory usage:                        1.49 KiB
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.one']
columns:                             ['system.one.dummy']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['TabSeparated']
used_functions:                      ['uptime','formatReadableTimeDelta']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66,225,281,283,282]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'IOBufferAllocs':3,'IOBufferAllocBytes':3145728,'SelectedRows':1,'SelectedBytes':1,'ContextLock':17,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':6367,'UserTimeMicroseconds':3329,'SystemTimeMicroseconds':531,'SoftPageFaults':6,'HardPageFaults':1,'OSCPUWaitMicroseconds':1090,'OSCPUVirtualTimeMicroseconds':3859,'OSReadBytes':102400,'OSReadChars':830,'OSWriteChars':1190}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 7:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:26:25
query_duration_ms:                   1
query_id:                            e9c25bd1-00d3-4239-9611-1c3d391178da
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT version()

read:                                1 rows / 1.00 B
written:                             0 rows / 0.00 B
result:                              1 rows / 128.00 B
memory usage:                        1.45 KiB
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.one']
columns:                             ['system.one.dummy']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['TabSeparated']
used_functions:                      ['version']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66,283,225,282]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'IOBufferAllocs':3,'IOBufferAllocBytes':3145728,'SelectedRows':1,'SelectedBytes':1,'ContextLock':15,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':2720,'UserTimeMicroseconds':648,'SystemTimeMicroseconds':1144,'OSCPUWaitMicroseconds':110,'OSCPUVirtualTimeMicroseconds':1790,'OSReadChars':845,'OSWriteChars':1140}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 8:
──────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:33:29
query_duration_ms:                   4
query_id:                            69762642-8a75-4149-aaf5-bc1969558747
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT version()

read:                                1 rows / 1.00 B
written:                             0 rows / 0.00 B
result:                              1 rows / 128.00 B
memory usage:                        1.45 KiB
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.one']
columns:                             ['system.one.dummy']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['TabSeparated']
used_functions:                      ['version']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66,282,283]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'IOBufferAllocs':3,'IOBufferAllocBytes':3145728,'SelectedRows':1,'SelectedBytes':1,'ContextLock':15,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':10137,'UserTimeMicroseconds':6289,'SystemTimeMicroseconds':47,'SoftPageFaults':2,'HardPageFaults':1,'OSCPUWaitMicroseconds':859,'OSCPUVirtualTimeMicroseconds':6336,'OSReadBytes':12288,'OSReadChars':845,'OSWriteChars':1140}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 9:
───────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:25:01
query_duration_ms:                   4
query_id:                            9e31242c-62c5-4bb1-9a3e-f96e99f3bddf
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT version()

read:                                1 rows / 1.00 B
written:                             0 rows / 0.00 B
result:                              1 rows / 128.00 B
memory usage:                        1.45 KiB
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.one']
columns:                             ['system.one.dummy']
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['TabSeparated']
used_functions:                      ['version']
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66,225,282,281,283]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'IOBufferAllocs':3,'IOBufferAllocBytes':3145728,'SelectedRows':1,'SelectedBytes':1,'ContextLock':15,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':8688,'UserTimeMicroseconds':3598,'SystemTimeMicroseconds':1288,'SoftPageFaults':42,'HardPageFaults':1,'OSCPUWaitMicroseconds':214,'OSCPUVirtualTimeMicroseconds':4885,'OSReadBytes':98304,'OSReadChars':818,'OSWriteChars':1140}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}

Row 10:
───────
type:                                QueryFinish
query_start_time:                    2021-12-25 15:26:26
query_duration_ms:                   2
query_id:                            de1fc64c-09c3-420a-8801-a2f9f04407cd
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT
        database,
    table,
    count() "partitions",
    sum(part_count) "parts",
    max(part_count) "max_parts_per_partition"
FROM
(
    SELECT
        database,
        table,
        partition,
        count() "part_count"
    FROM system.parts
    WHERE active
    GROUP BY database, table, partition
) partitions
GROUP BY database, table
ORDER BY max_parts_per_partition DESC
LIMIT 10 FORMAT PrettyCompactNoEscapes

read:                                12 rows / 643.00 B
written:                             0 rows / 0.00 B
result:                              6 rows / 752.00 B
memory usage:                        0.00 B
exception:
stack_trace:

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           ['system']
tables:                              ['system.parts']
columns:                             ['system.parts.active','system.parts.database','system.parts.partition','system.parts.table']
used_aggregate_functions:            ['count','max','sum']
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        ['PrettyCompactNoEscapes']
used_functions:                      []
used_storages:                       []
used_table_functions:                []
thread_ids:                          [66]
ProfileEvents:                       {'Query':1,'SelectQuery':1,'ArenaAllocChunks':2,'ArenaAllocBytes':8192,'SelectedRows':12,'SelectedBytes':643,'ContextLock':58,'RWLockAcquiredReadLocks':9,'RWLockReadersWaitMilliseconds':1,'RealTimeMicroseconds':2924,'UserTimeMicroseconds':1583,'SystemTimeMicroseconds':892,'SoftPageFaults':6,'OSCPUVirtualTimeMicroseconds':3423,'OSReadChars':438,'OSWriteChars':5086}
Settings:                            {'load_balancing':'random','max_memory_usage':'10000000000'}
```
##### Last 10 failed queries
**query**
```sql
SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    query_kind,
    is_initial_query,
    query,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) AS result,
    formatReadableSize(memory_usage) AS "memory usage",
    exception,
    '\n' || stack_trace AS stack_trace,
    user,
    initial_user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    client_hostname,
    databases,
    tables,
    columns,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_database_engines,
    used_data_type_families,
    used_dictionaries,
    used_formats,
    used_functions,
    used_storages,
    used_table_functions,
    thread_ids,
    ProfileEvents,
    Settings
    FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
  AND exception != ''
ORDER BY query_start_time DESC
LIMIT 10
```
**result**
```
Row 1:
──────
type:                                ExceptionBeforeStart
query_start_time:                    2021-12-25 15:33:29
query_duration_ms:                   0
query_id:                            323743ef-4dff-4ed3-9559-f405c64fbd4a
query_kind:                          Select
is_initial_query:                    1
query:                               SELECT
    '\n' || arrayStringConcat(
       arrayMap(
           x,
           y -> concat(x, ': ', y),
           arrayMap(x -> addressToLine(x), trace),
           arrayMap(x -> demangle(addressToSymbol(x)), trace)),
       '\n') AS trace
FROM system.stack_trace FORMAT Vertical

read:                                0 rows / 0.00 B
written:                             0 rows / 0.00 B
result:                              0 rows / 0.00 B
memory usage:                        0.00 B
exception:                           Code: 446. DB::Exception: default: Introspection functions are disabled, because setting 'allow_introspection_functions' is set to 0: While processing concat('\n', arrayStringConcat(arrayMap((x, y) -> concat(x, ': ', y), arrayMap(x -> addressToLine(x), trace), arrayMap(x -> demangle(addressToSymbol(x)), trace)), '\n')) AS trace. (FUNCTION_NOT_ALLOWED) (version 21.11.8.4 (official build))
stack_trace:
0. DB::Exception::Exception(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, int, bool) @ 0x9b682d4 in /usr/bin/clickhouse
1. bool DB::ContextAccess::checkAccessImplHelper<true, false>(DB::AccessFlags const&) const::'lambda'(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, int)::operator()(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, int) const @ 0x119786bc in /usr/bin/clickhouse
2. bool DB::ContextAccess::checkAccessImplHelper<true, false>(DB::AccessFlags const&) const @ 0x11977416 in /usr/bin/clickhouse
3. DB::Context::checkAccess(DB::AccessFlags const&) const @ 0x11eb2f08 in /usr/bin/clickhouse
4. ? @ 0xf96aefb in /usr/bin/clickhouse
5. DB::FunctionFactory::tryGetImpl(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, std::__1::shared_ptr<DB::Context const>) const @ 0x118f74b4 in /usr/bin/clickhouse
6. DB::FunctionFactory::getImpl(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, std::__1::shared_ptr<DB::Context const>) const @ 0x118f71fc in /usr/bin/clickhouse
7. DB::ActionsMatcher::visit(DB::ASTFunction const&, std::__1::shared_ptr<DB::IAST> const&, DB::ActionsMatcher::Data&) @ 0x120c3abf in /usr/bin/clickhouse
8. DB::ActionsMatcher::visit(DB::ASTFunction const&, std::__1::shared_ptr<DB::IAST> const&, DB::ActionsMatcher::Data&) @ 0x120c6b9f in /usr/bin/clickhouse
9. DB::ActionsMatcher::visit(DB::ASTFunction const&, std::__1::shared_ptr<DB::IAST> const&, DB::ActionsMatcher::Data&) @ 0x120c41ed in /usr/bin/clickhouse
10. DB::ActionsMatcher::visit(DB::ASTFunction const&, std::__1::shared_ptr<DB::IAST> const&, DB::ActionsMatcher::Data&) @ 0x120c41ed in /usr/bin/clickhouse
11. DB::ActionsMatcher::visit(DB::ASTFunction const&, std::__1::shared_ptr<DB::IAST> const&, DB::ActionsMatcher::Data&) @ 0x120c41ed in /usr/bin/clickhouse
12. DB::ActionsMatcher::visit(DB::ASTExpressionList&, std::__1::shared_ptr<DB::IAST> const&, DB::ActionsMatcher::Data&) @ 0x120ca818 in /usr/bin/clickhouse
13. DB::InDepthNodeVisitor<DB::ActionsMatcher, true, false, std::__1::shared_ptr<DB::IAST> const>::visit(std::__1::shared_ptr<DB::IAST> const&) @ 0x12099bb7 in /usr/bin/clickhouse
14. DB::ExpressionAnalyzer::getRootActions(std::__1::shared_ptr<DB::IAST> const&, bool, std::__1::shared_ptr<DB::ActionsDAG>&, bool) @ 0x120999cb in /usr/bin/clickhouse
15. DB::SelectQueryExpressionAnalyzer::appendSelect(DB::ExpressionActionsChain&, bool) @ 0x120a4409 in /usr/bin/clickhouse
16. DB::ExpressionAnalysisResult::ExpressionAnalysisResult(DB::SelectQueryExpressionAnalyzer&, std::__1::shared_ptr<DB::StorageInMemoryMetadata const> const&, bool, bool, bool, std::__1::shared_ptr<DB::FilterDAGInfo> const&, DB::Block const&) @ 0x120a9070 in /usr/bin/clickhouse
17. DB::InterpreterSelectQuery::getSampleBlockImpl() @ 0x1232fd0d in /usr/bin/clickhouse
18. ? @ 0x12328864 in /usr/bin/clickhouse
19. DB::InterpreterSelectQuery::InterpreterSelectQuery(std::__1::shared_ptr<DB::IAST> const&, std::__1::shared_ptr<DB::Context const>, std::__1::optional<DB::Pipe>, std::__1::shared_ptr<DB::IStorage> const&, DB::SelectQueryOptions const&, std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >, std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > > > const&, std::__1::shared_ptr<DB::StorageInMemoryMetadata const> const&, std::__1::unordered_map<DB::PreparedSetKey, std::__1::shared_ptr<DB::Set>, DB::PreparedSetKey::Hash, std::__1::equal_to<DB::PreparedSetKey>, std::__1::allocator<std::__1::pair<DB::PreparedSetKey const, std::__1::shared_ptr<DB::Set> > > >) @ 0x123232c7 in /usr/bin/clickhouse
20. DB::InterpreterSelectQuery::InterpreterSelectQuery(std::__1::shared_ptr<DB::IAST> const&, std::__1::shared_ptr<DB::Context const>, DB::SelectQueryOptions const&, std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >, std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > > > const&) @ 0x12321c54 in /usr/bin/clickhouse
21. DB::InterpreterSelectWithUnionQuery::buildCurrentChildInterpreter(std::__1::shared_ptr<DB::IAST> const&, std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >, std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > > > const&) @ 0x12547fa2 in /usr/bin/clickhouse
22. DB::InterpreterSelectWithUnionQuery::InterpreterSelectWithUnionQuery(std::__1::shared_ptr<DB::IAST> const&, std::__1::shared_ptr<DB::Context const>, DB::SelectQueryOptions const&, std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >, std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > > > const&) @ 0x12546680 in /usr/bin/clickhouse
23. DB::InterpreterFactory::get(std::__1::shared_ptr<DB::IAST>&, std::__1::shared_ptr<DB::Context>, DB::SelectQueryOptions const&) @ 0x122c6216 in /usr/bin/clickhouse
24. ? @ 0x1277dd26 in /usr/bin/clickhouse
25. DB::executeQuery(DB::ReadBuffer&, DB::WriteBuffer&, bool, std::__1::shared_ptr<DB::Context>, std::__1::function<void (std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&)>, std::__1::optional<DB::FormatSettings> const&) @ 0x12781319 in /usr/bin/clickhouse
26. DB::HTTPHandler::processQuery(DB::HTTPServerRequest&, DB::HTMLForm&, DB::HTTPServerResponse&, DB::HTTPHandler::Output&, std::__1::optional<DB::CurrentThread::QueryScope>&) @ 0x130c20fa in /usr/bin/clickhouse
27. DB::HTTPHandler::handleRequest(DB::HTTPServerRequest&, DB::HTTPServerResponse&) @ 0x130c6760 in /usr/bin/clickhouse
28. DB::HTTPServerConnection::run() @ 0x1312b5e8 in /usr/bin/clickhouse
29. Poco::Net::TCPServerConnection::start() @ 0x15d682cf in /usr/bin/clickhouse
30. Poco::Net::TCPServerDispatcher::run() @ 0x15d6a6c1 in /usr/bin/clickhouse
31. Poco::PooledThread::run() @ 0x15e7f069 in /usr/bin/clickhouse

user:                                default
initial_user:                        default
client:                              python-requests/2.26.0
client_hostname:
databases:                           []
tables:                              []
columns:                             []
used_aggregate_functions:            []
used_aggregate_function_combinators: []
used_database_engines:               []
used_data_type_families:             []
used_dictionaries:                   []
used_formats:                        []
used_functions:                      []
used_storages:                       []
used_table_functions:                []
thread_ids:                          []
ProfileEvents:                       {}
Settings:                            {}

```
#### Stack traces
**query**
```sql
SELECT
    '\n' || arrayStringConcat(
       arrayMap(
           x, y, z -> concat(x, ': ', y, ' @ ', z),
           arrayMap(x -> addressToLine(x), trace),
           arrayMap(x -> demangle(addressToSymbol(x)), trace),
           arrayMap(x -> '0x' || hex(x), trace)),
       '\n') AS trace
FROM system.stack_trace
```
**result**
```
Row 1:
──────
trace: 
:  @ 0x7F6694A91117
:  @ 0x7F6694A93A41
./build/./contrib/llvm-project/libcxx/src/condition_variable.cpp:47: std::__1::condition_variable::wait(std::__1::unique_lock<std::__1::mutex>&) @ 0x16F4A56F
./build/./contrib/llvm-project/libcxx/include/atomic:958: BaseDaemon::waitForTerminationRequest() @ 0x0B85564B
./build/./contrib/llvm-project/libcxx/include/vector:434: DB::Server::main(std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>, std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>>> const&) @ 0x0B6644CE
./build/./base/poco/Util/src/Application.cpp:0: Poco::Util::Application::run() @ 0x1489B8A6
./build/./programs/server/Server.cpp:402: DB::Server::run() @ 0x0B651E91
./build/./base/poco/Util/src/ServerApplication.cpp:132: Poco::Util::ServerApplication::run(int, char**) @ 0x148AF4F1
./build/./programs/server/Server.cpp:0: mainEntryClickHouseServer(int, char**) @ 0x0B64FA96
./build/./programs/main.cpp:0: main @ 0x06AB8C92
:  @ 0x7F6694A29D90
:  @ 0x7F6694A29E40
./build/./programs/clickhouse: _start @ 0x06AB802E

Row 2:
──────
trace: 
:  @ 0x7F6694B14A0C
./build/./src/IO/ReadBufferFromFileDescriptor.cpp:0: DB::ReadBufferFromFileDescriptor::readImpl(char*, unsigned long, unsigned long, unsigned long) @ 0x0B622EAB
./build/./src/IO/ReadBufferFromFileDescriptor.cpp:126: DB::ReadBufferFromFileDescriptor::nextImpl() @ 0x0B6231A0
./build/./src/IO/ReadBuffer.h:70: SignalListener::run() @ 0x0B85631D
./build/./base/poco/Foundation/include/Poco/SharedPtr.h:139: Poco::ThreadImpl::runnableEntry(void*) @ 0x149CA102
:  @ 0x7F6694A94AC3
:  @ 0x7F6694B26A40

```
#### uname
**command**
```
uname -a
```
**result**
```
Linux clickhouse01 5.10.76-linuxkit #1 SMP Mon Nov 8 10:21:19 UTC 2021 x86_64 x86_64 x86_64 GNU/Linux
```
