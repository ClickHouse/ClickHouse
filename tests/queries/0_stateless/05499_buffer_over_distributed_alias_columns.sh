#!/usr/bin/env bash
# Tags: distributed

# ALIAS columns of a Buffer table whose destination is a Distributed table, read in every query position.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --multiquery <<'EOF'
CREATE TABLE t_05499_dst (s String, n UInt32, a String ALIAS upper(s), b UInt32 ALIAS n, c UInt32 ALIAS b, w UInt64 ALIAS n, d UInt8 ALIAS n > 1, aa String ALIAS lower(a)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05499_dst VALUES ('x', 1), ('z', 3);
CREATE TABLE t_05499_dist1 AS t_05499_dst ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05499_dst);
CREATE TABLE t_05499_dist2 AS t_05499_dst ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_05499_dst);
CREATE TABLE t_05499_buf1 AS t_05499_dst ENGINE = Buffer(currentDatabase(), t_05499_dist1, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
CREATE TABLE t_05499_buf2 AS t_05499_dst ENGINE = Buffer(currentDatabase(), t_05499_dist2, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
CREATE TABLE t_05499_buf_empty AS t_05499_dst ENGINE = Buffer(currentDatabase(), t_05499_dist1, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO t_05499_buf1 VALUES ('y', 2);
INSERT INTO t_05499_buf2 VALUES ('y', 2);

SELECT '-- select list';
SELECT a, s FROM t_05499_buf1 ORDER BY ALL;
SELECT '-- empty buffers';
SELECT a, s FROM t_05499_buf_empty ORDER BY ALL;
SELECT '-- two shards';
SELECT s, a, b, c, d, w FROM t_05499_buf2 ORDER BY ALL;
SELECT '-- GROUP BY';
SELECT a, count() FROM t_05499_buf1 GROUP BY a ORDER BY ALL;
SELECT '-- aggregate argument';
SELECT sum(length(a)), count() FROM t_05499_buf1;
SELECT '-- ORDER BY';
SELECT s FROM t_05499_buf1 ORDER BY a;
SELECT '-- ALIAS kinds';
SELECT b, c, w, toTypeName(w), d, aa FROM t_05499_buf1 ORDER BY ALL;
SELECT '-- ALIAS equal to another selected expression';
SELECT a, upper(s), n, b FROM t_05499_buf1 ORDER BY ALL;
SELECT a, upper(s), count() FROM t_05499_buf1 GROUP BY a, upper(s) ORDER BY ALL;

SELECT '-- duplicate GROUP BY keys with two-level aggregation';
CREATE TABLE t_05499_dst_big (s String, a String ALIAS upper(s)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_05499_dist_big AS t_05499_dst_big ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05499_dst_big);
CREATE TABLE t_05499_buf_big AS t_05499_dst_big ENGINE = Buffer(currentDatabase(), t_05499_dist_big, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO t_05499_dst_big SELECT toString(number) FROM numbers(1000);
INSERT INTO t_05499_buf_big SELECT toString(number) FROM numbers(1000);
-- Both halves aggregate two-level, so equal groups merge only if both halves bucket by the same keys.
SELECT count(), countIf(c = 2) FROM (SELECT a, upper(s), count() AS c FROM t_05499_buf_big GROUP BY a, upper(s))
SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1, distributed_aggregation_memory_efficient = 1;
SELECT count(), countIf(c = 2) FROM (SELECT a, upper(s), count() AS c FROM t_05499_buf_big GROUP BY a, upper(s))
SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1, distributed_aggregation_memory_efficient = 0;

SELECT '-- nested in a JOIN';
SELECT q.z, t.a FROM (SELECT 1 AS z) AS q CROSS JOIN (SELECT a, s FROM t_05499_buf1 ORDER BY s LIMIT 5) AS t ORDER BY ALL;
SELECT '-- distributed_group_by_no_merge';
SELECT a, count() FROM t_05499_buf1 GROUP BY a ORDER BY ALL SETTINGS distributed_group_by_no_merge = 1;
SELECT '-- prefer_localhost_replica = 0';
SELECT a, s FROM t_05499_buf1 ORDER BY ALL SETTINGS prefer_localhost_replica = 0;
SELECT '-- merge() over the Buffer';
SELECT a, b, s FROM merge(currentDatabase(), '^t_05499_buf1$') ORDER BY ALL;

SELECT '-- Buffer over Merge over Distributed';
CREATE TABLE t_05499_merge_d AS t_05499_dst ENGINE = Merge(currentDatabase(), '^t_05499_dist2$');
CREATE TABLE t_05499_buf_merge AS t_05499_dst ENGINE = Buffer(currentDatabase(), t_05499_merge_d, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO t_05499_buf_merge VALUES ('m', 5);
SELECT a, s FROM t_05499_buf_merge ORDER BY ALL;
SELECT a, count() FROM t_05499_buf_merge GROUP BY a ORDER BY ALL;
EOF

echo "-- a grant on the ALIAS column only"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${USER}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.t_05499_buf1 TO ${USER}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05499_dist1 TO ${USER}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05499_dst TO ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "SELECT a FROM t_05499_buf1 ORDER BY a"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "SELECT s FROM t_05499_buf1" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

${CLICKHOUSE_CLIENT} --multiquery <<'EOF'
-- DROP flushes the buffers, which a Merge or a two-shard Distributed without a sharding key cannot accept.
SET send_logs_level = 'fatal';
DROP TABLE t_05499_buf_merge;
DROP TABLE t_05499_merge_d;
DROP TABLE t_05499_buf_big;
DROP TABLE t_05499_dist_big;
DROP TABLE t_05499_dst_big;
DROP TABLE t_05499_buf_empty;
DROP TABLE t_05499_buf2;
DROP TABLE t_05499_buf1;
DROP TABLE t_05499_dist2;
DROP TABLE t_05499_dist1;
DROP TABLE t_05499_dst;
EOF
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER}"
