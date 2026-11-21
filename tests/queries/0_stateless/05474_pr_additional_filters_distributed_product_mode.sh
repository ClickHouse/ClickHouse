#!/usr/bin/env bash

# `distributed_product_mode` rewrites an `IN` subquery over a `Distributed` table before parallel replicas receive the
# query as text: `local` makes the replicas read the remote table, so an `additional_table_filters` entry keyed by the
# `Distributed` table is refused, and `global` (or `prefer_global_in_and_join`) runs the subquery on the initiator under
# its own map, so parallel replicas stay in use.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
SET automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_allow_in_with_subquery = 1,
    serialize_query_plan = 1, parallel_replicas_local_plan = 0, parallel_replicas_plan_based = 0;

CREATE TABLE t_outer (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_inner (y UInt64) ENGINE = MergeTree ORDER BY y;
CREATE TABLE t_dist (y UInt64) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), 't_inner');
INSERT INTO t_outer SELECT number FROM numbers(10);
INSERT INTO t_inner SELECT number FROM numbers(10);

SELECT count(), sum(x) FROM t_outer WHERE x IN (SELECT y FROM t_dist)
SETTINGS additional_table_filters = {'${CLICKHOUSE_DATABASE}.t_dist': 'y <= 2'}, distributed_product_mode = 'local',
    enable_parallel_replicas = 2; -- { serverError SUPPORT_IS_DISABLED }
SELECT count(), sum(x) FROM t_outer WHERE x IN (SELECT y FROM t_dist)
SETTINGS additional_table_filters = {'${CLICKHOUSE_DATABASE}.t_dist': 'y <= 2'}, distributed_product_mode = 'local',
    enable_parallel_replicas = 1;
-- Read directly, the Distributed table is not replaced.
SELECT count(), sum(y) FROM t_dist
SETTINGS additional_table_filters = {'${CLICKHOUSE_DATABASE}.t_dist': 'y <= 2'}, distributed_product_mode = 'local',
    enable_parallel_replicas = 2;

SELECT count(), sum(x) FROM t_outer
WHERE x IN (SELECT y FROM t_dist AS a SETTINGS enable_parallel_replicas = 0, additional_table_filters = {'a': 'y <= 2'})
SETTINGS distributed_product_mode = 'global', enable_parallel_replicas = 2, log_comment = '05474_global';
SELECT count(), sum(x) FROM t_outer
WHERE x IN (SELECT y FROM t_dist AS a SETTINGS enable_parallel_replicas = 0, additional_table_filters = {'a': 'y <= 2'})
SETTINGS prefer_global_in_and_join = 1, enable_parallel_replicas = 2, log_comment = '05474_prefer_global';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, argMax(ProfileEvents['ParallelReplicasUsedCount'], event_time_microseconds) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
    AND log_comment IN ('05474_global', '05474_prefer_global')
GROUP BY log_comment
ORDER BY log_comment
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_dist;
DROP TABLE t_outer;
DROP TABLE t_inner;
"
