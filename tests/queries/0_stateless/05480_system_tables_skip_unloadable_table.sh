#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table that fails to load at startup is skipped by the system tables instead of failing every query to them.
# clickhouse-local loads the persisted tables asynchronously, like the server. The `Set` table takes a noticeable
# time to load, and the system tables must still wait for it.

data_dir="${CLICKHOUSE_TMP}/data_${CLICKHOUSE_DATABASE}"
rm -rf "$data_dir"

$CLICKHOUSE_LOCAL --path "$data_dir" --query "
    SET enable_time_series_table = 1;
    CREATE DATABASE a;
    CREATE TABLE a.good (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO a.good VALUES (1);
    CREATE TABLE a.slow (x UInt64) ENGINE = Set;
    INSERT INTO a.slow SELECT number FROM numbers(1000000);
    CREATE TABLE a.broken (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS object_shared_data_serialization_version = 'advanced';
    CREATE TABLE a.source (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW a.mv TO a.broken AS SELECT x FROM a.source;
    CREATE MATERIALIZED VIEW a.rmv REFRESH EVERY 1 YEAR APPEND TO a.broken EMPTY AS SELECT x FROM a.good;
    CREATE TABLE a.merge (x UInt64) ENGINE = Merge(a, '^broken');
    CREATE TABLE a.alias ENGINE = Alias(a, broken);
    CREATE TABLE a.alias_merge ENGINE = Alias(a, merge);
    CREATE TABLE a.broken_families (metric_family String, type String, unit String, help String) ENGINE = MergeTree ORDER BY metric_family SETTINGS object_shared_data_serialization_version = 'advanced';
    CREATE DATABASE b;
    CREATE TABLE b.ts ENGINE = TimeSeries SETTINGS version = 9 METRIC FAMILIES a.broken_families;
    CREATE DATABASE c;
    CREATE TABLE c.source (y String) ENGINE = MergeTree ORDER BY y;
    CREATE TABLE c.only_c (y String) ENGINE = MergeTree ORDER BY y;
    CREATE DATABASE o ENGINE = Overlay('a', 'c');
    CREATE TABLE good_in_default (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE broken_in_default (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS object_shared_data_serialization_version = 'advanced';
"

# A value this version does not know, as in a table created by a newer version. The startup of the views writing
# to `a.broken`, and of the source of `a.mv`, is then canceled, so they are skipped too.
for f in $(find "$data_dir/store" -name 'broken*.sql'); do
    sed -i "s/object_shared_data_serialization_version = 'advanced'/object_shared_data_serialization_version = 'from_a_newer_version'/" "$f"
done

error=$($CLICKHOUSE_LOCAL --path "$data_dir" --query "SELECT * FROM a.broken" 2>&1)
echo "$error" | grep -o -m1 "Unexpected value of MergeTreeObjectSharedDataSerializationVersion: 'from_a_newer_version'"
echo "$error" | grep -o -m1 'ASYNC_LOAD_WAIT_FAILED'

$CLICKHOUSE_LOCAL --path "$data_dir" --query "
    SELECT name, engine FROM system.tables WHERE database = 'a' ORDER BY name;
    SELECT name FROM system.tables WHERE database = 'a' AND engine = 'MergeTree';
    SHOW TABLES FROM a;
    SHOW TABLES;
    SELECT count() > 0 FROM system.tables;
    SELECT * FROM system.tables FORMAT Null;
    SELECT table, sum(rows) FROM system.parts WHERE database = 'a' AND active GROUP BY table;
    SELECT table, name FROM system.columns WHERE database = 'a' ORDER BY table, name;
    SELECT * FROM system.columns FORMAT Null;
    SELECT table, name, data_compressed_bytes FROM system.columns WHERE database = 'a' AND table = 'alias_merge';
    SELECT name, data_paths FROM system.tables WHERE database = 'b' AND name = 'ts';
    SELECT 'overlay', name, engine FROM system.tables WHERE database = 'o' AND name IN ('source', 'only_c');
    SELECT 'overlay', name FROM system.tables WHERE database = 'o' AND name IN ('source', 'only_c');
    SELECT 'overlay', table, name, type FROM system.columns WHERE database = 'o' AND table IN ('source', 'only_c');
    SELECT 'parts_columns', count() FROM system.parts_columns WHERE database = 'a' AND active;
    SELECT 'projection_parts', count() FROM system.projection_parts WHERE database = 'a';
    SELECT 'mutations', count() FROM system.mutations WHERE database = 'a';
    SELECT 'replicas', count() FROM system.replicas WHERE database = 'a';
    SELECT 'replication_queue', count() FROM system.replication_queue WHERE database = 'a';
    SELECT 'projections', count() FROM system.projections WHERE database = 'a';
    SELECT 'data_skipping_indices', count() FROM system.data_skipping_indices WHERE database = 'a';
    SELECT 'constraints', count() FROM system.constraints WHERE database = 'a';
    SELECT 'distribution_queue', count() FROM system.distribution_queue WHERE database = 'a';
    SELECT 'part_moves_between_shards', count() FROM system.part_moves_between_shards WHERE database = 'a';
    SELECT 'graphite_retentions', count() FROM system.graphite_retentions;
    SELECT 'completions', count() > 0 FROM system.completions WHERE word = 'good';
    SELECT status, exception LIKE '%MergeTreeObjectSharedDataSerializationVersion%' FROM system.asynchronous_loader WHERE job = 'load table a.broken';
    SELECT * FROM a.good;
"

rm -rf "$data_dir"
