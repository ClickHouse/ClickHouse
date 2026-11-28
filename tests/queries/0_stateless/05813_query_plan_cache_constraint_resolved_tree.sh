#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# The constraints folded into a plan by the constraint-based optimizations are checked for query plan
# cache eligibility on their resolved query tree (`ConstraintsDescription::getQueryTreeData`), in addition
# to the raw constraint AST. A deterministic constraint must stay cacheable under that check, while a
# constraint calling a non-deterministic function must not be cached (see also 05455).
# The plan cache is a single, server-wide cache, so the test runs in isolation (see 04489 for the full
# rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

$CLICKHOUSE_CLIENT --query "
    SET allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1;
    SET convert_query_to_cnf = 1, optimize_using_constraints = 1;
    DROP TABLE IF EXISTS t_deterministic;
    DROP TABLE IF EXISTS t_non_deterministic;
    CREATE TABLE t_deterministic (b UInt64, c UInt64, CONSTRAINT c1 ASSUME b <= 10, CONSTRAINT c2 ASSUME c = b + 1) ENGINE = MergeTree ORDER BY b;
    CREATE TABLE t_non_deterministic (d Date, CONSTRAINT c1 ASSUME d <= today()) ENGINE = MergeTree ORDER BY d;
    INSERT INTO t_deterministic VALUES (7, 8);
    INSERT INTO t_non_deterministic VALUES ('2000-01-01');
    SELECT /* deterministic */ b, c FROM t_deterministic WHERE b > 20 OR b < 8 ORDER BY b;
    SELECT /* deterministic */ b, c FROM t_deterministic WHERE b > 20 OR b < 8 ORDER BY b;
    SELECT /* non_deterministic */ d FROM t_non_deterministic WHERE d > '2100-01-01' OR d < '2001-01-01' ORDER BY d;
    SELECT /* non_deterministic */ d FROM t_non_deterministic WHERE d > '2100-01-01' OR d < '2001-01-01' ORDER BY d;
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
echo "-- plan cache hits per query (only the deterministic constraint is cached)"
$CLICKHOUSE_CLIENT --query "
    SELECT extract(query, '/\\* (\\w+) \\*/') AS name, count(), sum(ProfileEvents['QueryPlanCacheHits'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT /* %'
    GROUP BY name
    ORDER BY name"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_deterministic; DROP TABLE t_non_deterministic"
