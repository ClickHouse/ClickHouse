#!/usr/bin/env bash
# Tags: shard

# A filter that the shard applies from `additional_table_filters` can read other tables of the shard, and a change
# of only those tables changes what a read through a `Distributed` table returns, while the hash of the underlying
# table and the text of the filter stay the same. The remote probe of the modification hash cannot validate such a
# filter, so it fails closed. With `prefer_localhost_replica = 0` the local shard is probed over TCP too.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

filter="additional_table_filters = {'${CLICKHOUSE_DATABASE}.dist_filter_src': 'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.dist_filter_helper)'}"
qc="use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1"

${CLICKHOUSE_CLIENT} --prefer_localhost_replica 0 --serialize_query_plan 0 -n -q "
DROP TABLE IF EXISTS dist_filter_src;
DROP TABLE IF EXISTS dist_filter_helper;
DROP TABLE IF EXISTS dist_filter;

CREATE TABLE dist_filter_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO dist_filter_src VALUES (1), (2), (3);
CREATE TABLE dist_filter_helper (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO dist_filter_helper VALUES (1);

CREATE TABLE dist_filter (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_filter_src);

SELECT 'no filter', modification_hash IS NOT NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'dist_filter';

SELECT 'filter reading another table', modification_hash IS NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'dist_filter'
SETTINGS ${filter};

SELECT count() FROM dist_filter SETTINGS ${filter}, ${qc};
INSERT INTO dist_filter_helper VALUES (2);
-- Only the table read by the filter changed: the fresh count, not the cached one.
SELECT count() FROM dist_filter SETTINGS ${filter}, ${qc};

DROP TABLE dist_filter;
DROP TABLE dist_filter_helper;
DROP TABLE dist_filter_src;
"
