#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs MinIO

# An `INSERT ... SELECT FROM <cluster function>` that inserts on the initiator must fill the fields missing in the
# files with the `DEFAULT` expressions of the destination table, the same as a plain `INSERT ... SELECT FROM s3(...)`.
# All nodes of the clusters below are this server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

S3_DIR="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05473"
CLUSTER="test_cluster_one_shard_three_replicas_localhost"
S3_GLOB="${S3_DIR}/part_{1,2}.jsonl"
PARALLEL_REPLICAS="enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = '${CLUSTER}', parallel_replicas_for_cluster_engines = 1"

function write_file()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${S3_DIR}/$1', 'JSONEachRow', '$2') SETTINGS s3_truncate_on_insert = 1 VALUES $3"
}

# The first file has none of the defaulted fields, the second one has all of them.
write_file part_1.jsonl "id UInt32" "(1), (3)"
write_file part_2.jsonl "id UInt32, d Date, q UInt32, e UInt32" "(2, '2001-02-03', 5, 6)"

COLUMNS="id UInt32, d Date DEFAULT '2000-01-01', q UInt32 DEFAULT 7, e UInt32 DEFAULT id * 10"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS local_05473;
    DROP TABLE IF EXISTS dist_key_05473;
    DROP TABLE IF EXISTS dist_rand_05473;
    CREATE TABLE local_05473 (${COLUMNS}) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE dist_key_05473 AS local_05473 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_05473, id);
    CREATE TABLE dist_rand_05473 AS local_05473 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_05473, rand());
"

function check()
{
    echo "--- $1 ---"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM local_05473 ORDER BY id"
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE local_05473"
}

# A deterministic sharding key makes the parallel distributed `INSERT SELECT` fall back to the ordinary one.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO dist_key_05473 SELECT * FROM s3Cluster('${CLUSTER}', '${S3_GLOB}', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1"
check "Distributed with a deterministic sharding key"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO dist_rand_05473 SELECT * FROM s3Cluster('${CLUSTER}', '${S3_GLOB}', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 0, distributed_foreground_insert = 1"
check "parallel_distributed_insert_select = 0"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO local_05473 SELECT * FROM s3Cluster('${CLUSTER}', '${S3_GLOB}', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2"
check "non-replicated destination"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO dist_key_05473 SELECT * FROM s3('${S3_GLOB}', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1, ${PARALLEL_REPLICAS}"
check "s3 with parallel replicas into Distributed with a deterministic sharding key"

# The `DEFAULT` of a structure written by the user is kept too.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO local_05473 (id, q) SELECT * FROM s3Cluster('${CLUSTER}', '${S3_GLOB}', format = 'JSONEachRow', structure = 'id UInt32, q UInt32 DEFAULT 42')
    SETTINGS parallel_distributed_insert_select = 0"
check "explicit structure with a DEFAULT as key-value argument"

# With the default values of the types the rows got `1970-01-01` and the TTL deleted them.
write_file events.jsonl "id UInt32, event_time DateTime" "(1, now()), (2, now())"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS events_local_05473;
    DROP TABLE IF EXISTS events_dist_05473;
    CREATE TABLE events_local_05473 (id UInt32, event_time DateTime, event_date Date DEFAULT toDate(event_time))
    ENGINE = MergeTree ORDER BY id TTL event_date + INTERVAL 30 DAY;
    CREATE TABLE events_dist_05473 AS events_local_05473 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), events_local_05473, id);
"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO events_dist_05473 SELECT * FROM s3Cluster('${CLUSTER}', '${S3_DIR}/events.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE events_local_05473 FINAL"
echo "--- DEFAULT reading a column of the files, with TTL ---"
$CLICKHOUSE_CLIENT -q "SELECT count(), min(event_date) > '2020-01-01' FROM events_local_05473"

# A `DEFAULT` reading a column the nodes do not have, or one whose `DEFAULT` they do not get, is not sent.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mat_05473;
    CREATE TABLE mat_05473 (id UInt32, m UInt32 MATERIALIZED id + 100, x UInt32 DEFAULT m + 1, b UInt32 DEFAULT x + 1, q UInt32 DEFAULT 7)
    ENGINE = MergeTree ORDER BY id;
"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO mat_05473 SELECT * FROM s3Cluster('${CLUSTER}', '${S3_DIR}/part_1.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 0"
echo "--- DEFAULT reading a MATERIALIZED column ---"
$CLICKHOUSE_CLIENT -q "SELECT id, q, b != 1 FROM mat_05473 ORDER BY id"

write_file sub.jsonl "id UInt32, payload Tuple(v UInt32)" "(1, tuple(5))"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS sub_05473;
    CREATE TABLE sub_05473 (id UInt32, payload Tuple(v UInt32), d UInt32 DEFAULT payload.v) ENGINE = MergeTree ORDER BY id;
"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO sub_05473 SELECT * FROM s3Cluster('${CLUSTER}', '${S3_DIR}/sub.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 0"
echo "--- DEFAULT reading a subcolumn ---"
$CLICKHOUSE_CLIENT -q "SELECT id, d FROM sub_05473"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS dict_dst_05473;
    DROP DICTIONARY IF EXISTS dict_05473;
    DROP TABLE IF EXISTS dict_src_05473;
    DROP TABLE IF EXISTS join_05473;
    DROP TABLE IF EXISTS set_05473;
    CREATE TABLE dict_src_05473 (id UInt32, v UInt32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO dict_src_05473 VALUES (1, 100), (3, 300);
    CREATE DICTIONARY dict_05473 (id UInt32, v UInt32) PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'dict_src_05473' DB '${CLICKHOUSE_DATABASE}')) LAYOUT(FLAT()) LIFETIME(0);
    CREATE TABLE join_05473 (id UInt32, w UInt32) ENGINE = Join(ANY, LEFT, id);
    INSERT INTO join_05473 VALUES (1, 10), (3, 30);
    CREATE TABLE set_05473 (id UInt32) ENGINE = Set;
    INSERT INTO set_05473 VALUES (1), (3);
    CREATE TABLE dict_dst_05473 (id UInt32, v UInt32 DEFAULT dictGet(${CLICKHOUSE_DATABASE}.dict_05473, 'v', id),
        w UInt32 DEFAULT joinGet(${CLICKHOUSE_DATABASE}.join_05473, 'w', id),
        s UInt8 DEFAULT id IN ${CLICKHOUSE_DATABASE}.set_05473) ENGINE = MergeTree ORDER BY id;
"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO dict_dst_05473 SELECT * FROM s3Cluster('${CLUSTER}', '${S3_DIR}/part_1.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 0"
echo "--- DEFAULT reading a dictionary, a Join table and a Set table by identifier ---"
$CLICKHOUSE_CLIENT -q "SELECT id, v, w, s FROM dict_dst_05473 ORDER BY id"

# A `DEFAULT` written in the `structure` argument keeps a plain identifier on the right of `IN`.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS in_05473;
    CREATE TABLE in_05473 (id UInt32, s UInt8) ENGINE = MergeTree ORDER BY id;
"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO in_05473 SELECT * FROM s3Cluster('${CLUSTER}', '${S3_DIR}/part_1.jsonl', format = 'JSONEachRow',
        structure = 'id UInt32, s UInt8 DEFAULT id IN ${CLICKHOUSE_DATABASE}.set_05473')
    SETTINGS parallel_distributed_insert_select = 0"
echo "--- explicit structure with a DEFAULT reading a Set table by identifier ---"
$CLICKHOUSE_CLIENT -q "SELECT id, s FROM in_05473 ORDER BY id"

# A `DEFAULT` with a secret argument is not sent, so the key never reaches the logs of the nodes.
write_file sec.jsonl "id UInt32, p String" "(1, 'a')"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS sec_05473;
    CREATE TABLE sec_05473 (id UInt32, p String, h String DEFAULT HMAC('sha256', p, 'k3y_05473')) ENGINE = MergeTree ORDER BY id;
"
$CLICKHOUSE_CLIENT --query_id "${CLICKHOUSE_DATABASE}_05473_secret" -q "
    INSERT INTO sec_05473 SELECT * FROM s3Cluster('${CLUSTER}', '${S3_DIR}/sec.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 0"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
echo "--- DEFAULT with a secret argument ---"
$CLICKHOUSE_CLIENT -q "SELECT countIf(query LIKE '%k3y_05473%'), count() > 0 FROM system.query_log
    WHERE event_date >= yesterday() AND initial_query_id = '${CLICKHOUSE_DATABASE}_05473_secret' AND NOT is_initial_query
    AND current_database IN ['default', currentDatabase()]"

# `SELECT a, x, ...` binds the column `x` of the files to the column `y` of the table, so `DEFAULT x` must not read it.
write_file col.jsonl "a UInt32, x UInt32" "(1, 2)"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS col_05473;
    DROP TABLE IF EXISTS col_dist_05473;
    DROP TABLE IF EXISTS col_input_05473;
    CREATE TABLE col_05473 (x UInt32, y UInt32, z UInt32 DEFAULT x, w UInt32 DEFAULT z + 10) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE col_dist_05473 AS col_05473 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), col_05473, x);
    CREATE TABLE col_input_05473 AS col_05473;
"

function check_col()
{
    echo "--- $1 ---"
    $CLICKHOUSE_CLIENT -q "SELECT z != y, w != 10 FROM $2"
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE $2"
}

$CLICKHOUSE_CLIENT -q "
    INSERT INTO col_05473 SELECT a, x, z, w FROM s3('${S3_DIR}/col.jsonl', 'JSONEachRow')
    SETTINGS parallel_replicas_for_cluster_engines = 0"
check_col "DEFAULT reading a column bound to another column, s3" col_05473

$CLICKHOUSE_CLIENT -q "
    INSERT INTO col_dist_05473 SELECT a, x, z, w FROM s3Cluster('${CLUSTER}', '${S3_DIR}/col.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1"
check_col "DEFAULT reading a column bound to another column, s3Cluster" col_05473

echo '{"a":1,"x":2}' | $CLICKHOUSE_CLIENT -q "INSERT INTO col_input_05473 SELECT a, x, z, w FROM input() FORMAT JSONEachRow"
check_col "DEFAULT reading a column bound to another column, input()" col_input_05473

# `SELECT y, y, z` binds the column `y` of the files to both `x` and `y` of the table, so `DEFAULT y + 1` reads it.
write_file dup.jsonl "y UInt32" "(5)"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS dup_dist_05473;
    DROP TABLE IF EXISTS dup_05473;
    DROP TABLE IF EXISTS dup_input_05473;
    CREATE TABLE dup_05473 (x UInt32, y UInt32, z UInt32 DEFAULT y + 1) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE dup_input_05473 AS dup_05473;
    CREATE TABLE dup_dist_05473 AS dup_05473 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), dup_05473, x);
"

function check_dup()
{
    echo "--- $1 ---"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM $2"
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE $2"
}

$CLICKHOUSE_CLIENT -q "
    INSERT INTO dup_05473 SELECT y, y, z FROM s3('${S3_DIR}/dup.jsonl', 'JSONEachRow')
    SETTINGS parallel_replicas_for_cluster_engines = 0"
check_dup "column selected twice, s3" dup_05473

$CLICKHOUSE_CLIENT -q "
    INSERT INTO dup_05473 SELECT y, y, z FROM s3Cluster('${CLUSTER}', '${S3_DIR}/dup.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 0"
check_dup "column selected twice, s3Cluster" dup_05473

$CLICKHOUSE_CLIENT -q "
    INSERT INTO dup_dist_05473 SELECT y, y, z FROM s3Cluster('${CLUSTER}', '${S3_DIR}/dup.jsonl', 'JSONEachRow')
    SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1"
check_dup "column selected twice, s3Cluster into Distributed with a deterministic sharding key" dup_05473

echo '{"y":5}' | $CLICKHOUSE_CLIENT -q "INSERT INTO dup_input_05473 SELECT y, y, z FROM input() FORMAT JSONEachRow"
check_dup "column selected twice, input()" dup_input_05473

$CLICKHOUSE_CLIENT -q "
    DROP TABLE dist_key_05473;
    DROP TABLE dist_rand_05473;
    DROP TABLE local_05473;
    DROP TABLE events_dist_05473;
    DROP TABLE events_local_05473;
    DROP TABLE mat_05473;
    DROP TABLE sub_05473;
    DROP TABLE dict_dst_05473;
    DROP TABLE in_05473;
    DROP TABLE set_05473;
    DROP DICTIONARY dict_05473;
    DROP TABLE dict_src_05473;
    DROP TABLE join_05473;
    DROP TABLE sec_05473;
    DROP TABLE col_dist_05473;
    DROP TABLE col_05473;
    DROP TABLE col_input_05473;
    DROP TABLE dup_dist_05473;
    DROP TABLE dup_05473;
    DROP TABLE dup_input_05473;
"
