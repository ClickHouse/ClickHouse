#!/usr/bin/env bash
# Tags: atomic-database

# Edge cases of STREAM from a `Merge` table and of an incremental refresh from it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

DB=$CLICKHOUSE_DATABASE
DOT_DB="${CLICKHOUSE_DATABASE}.dot"

echo "--- a table name with a dot"
$STREAMING_CLIENT -q "
    CREATE TABLE \`t.dot\` (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS;
    INSERT INTO \`t.dot\` VALUES (1), (2);
    INSERT INTO \`t.dot\` VALUES (3);
    SELECT _table, a FROM merge('^t\\\\.dot\$') STREAM BOUNDED CURSOR {'$DB': {'t.dot': {'all': {'block_number': 1, 'block_offset': 1}}}} ORDER BY a;
"

echo "--- a database name with a dot"
$STREAMING_CLIENT -q "
    DROP DATABASE IF EXISTS \`$DOT_DB\`;
    CREATE DATABASE \`$DOT_DB\`;
    CREATE TABLE \`$DOT_DB\`.t (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS;
    INSERT INTO \`$DOT_DB\`.t VALUES (1), (2);
    INSERT INTO \`$DOT_DB\`.t VALUES (3);
    SELECT _table, a FROM merge('$DOT_DB', '^t\$') STREAM BOUNDED CURSOR {'$DOT_DB': {'t': {'all': {'block_number': 1, 'block_offset': 1}}}} ORDER BY a;
    DROP DATABASE \`$DOT_DB\`;
"

echo "--- a nested Merge table"
$STREAMING_CLIENT -q "
    CREATE TABLE n1 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS;
    CREATE TABLE m_inner (a UInt64) ENGINE = Merge(currentDatabase(), '^n1\$');
    CREATE TABLE m_outer (a UInt64) ENGINE = Merge(currentDatabase(), '^m_inner\$');
"
$STREAMING_CLIENT -q "SELECT a FROM m_outer STREAM BOUNDED" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"

echo "--- parallel replicas do not expand a streaming read"
$STREAMING_CLIENT -q "
    CREATE TABLE r1 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS;
    CREATE TABLE r2 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS;
    INSERT INTO r1 VALUES (1);
    INSERT INTO r2 VALUES (42);
"
$STREAMING_CLIENT -q "
    SELECT _table, a FROM merge('^r[12]\$') STREAM CURSOR {'$DB': {'r1': {'all': {'block_number': 1, 'block_offset': 0}}}} LIMIT 1
    SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, parallel_replicas_allow_merge_tables = 1,
        parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_streams_for_union_step = 1
"

echo "--- incremental refresh from a table without persisted block columns"
$STREAMING_CLIENT -q "
    CREATE TABLE u1 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0;
    CREATE TABLE mu (a UInt64) ENGINE = Merge(currentDatabase(), '^u1\$');
    CREATE TABLE tgt_u (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW mv_u REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt_u EMPTY AS SELECT a FROM mu;
    SYSTEM REFRESH VIEW mv_u;
"
$STREAMING_CLIENT -q "SYSTEM WAIT VIEW mv_u" 2>&1 | grep -o -m1 "must set enable_block_number_column = 1 and enable_block_offset_column = 1"
$STREAMING_CLIENT -q "DROP TABLE mv_u"

echo "--- incremental refresh from a ReplacingMergeTree table"
$STREAMING_CLIENT -q "
    CREATE TABLE rmt (a UInt64) ENGINE = ReplacingMergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS;
    CREATE TABLE mr (a UInt64) ENGINE = Merge(currentDatabase(), '^rmt\$');
    CREATE TABLE tgt_r (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW mv_r REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt_r EMPTY AS SELECT a FROM mr;
    SYSTEM REFRESH VIEW mv_r;
"
$STREAMING_CLIENT -q "SYSTEM WAIT VIEW mv_r" 2>&1 | grep -o -m1 "uses a merging engine"
$STREAMING_CLIENT -q "DROP TABLE mv_r"

echo "--- incremental refresh after a table is dropped and created again"
$STREAMING_CLIENT -q "
    CREATE TABLE c1 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS;
    CREATE TABLE mc (a UInt64) ENGINE = Merge(currentDatabase(), '^c1\$');
    CREATE TABLE tgt_c (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW mv_c REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt_c EMPTY AS SELECT a FROM mc;
    INSERT INTO c1 VALUES (1);
    INSERT INTO c1 VALUES (2);
    SYSTEM REFRESH VIEW mv_c;
    SYSTEM WAIT VIEW mv_c;
    SELECT 'before', arraySort(groupArray(a)) FROM tgt_c;
    DROP TABLE c1 SYNC;
    CREATE TABLE c1 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS;
    INSERT INTO c1 VALUES (100);
    SYSTEM REFRESH VIEW mv_c;
    SYSTEM WAIT VIEW mv_c;
    SELECT 'after', arraySort(groupArray(a)) FROM tgt_c;
    INSERT INTO c1 VALUES (200);
    SYSTEM REFRESH VIEW mv_c;
    SYSTEM WAIT VIEW mv_c;
    SELECT 'next', arraySort(groupArray(a)) FROM tgt_c;
    DROP TABLE mv_c;
"
