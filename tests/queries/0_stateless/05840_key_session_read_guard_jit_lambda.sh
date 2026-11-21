#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# The lambda body of a key is JIT-compiled under the server settings, and `greatest` over a nullable argument is
# compilable only with `least_greatest_legacy_null_behavior = 1`. A query that sets it differently computes other
# key values, so it must not use the key, also when `greatest` is hidden inside the compiled function.
# https://github.com/ClickHouse/ClickHouse/pull/109196

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --least_greatest_legacy_null_behavior=1 --compile_expressions=1 --min_count_to_compile_expression=0 --multiquery "
CREATE TABLE t_folded (x Array(Nullable(UInt32)), y Array(Nullable(UInt32)), v UInt32)
ENGINE = MergeTree ORDER BY arrayCount(z -> isNull(z), arrayMap((a, b) -> greatest(a, b) + 1, x, y))
SETTINGS allow_nullable_key = 1, index_granularity = 1;
SYSTEM STOP MERGES t_folded;
INSERT INTO t_folded VALUES ([NULL], [1], 1);
INSERT INTO t_folded VALUES ([5], [1], 2);

CREATE TABLE t_captured (x Array(Nullable(UInt32)), w Nullable(UInt32), v UInt32)
ENGINE = MergeTree ORDER BY arrayCount(z -> isNull(z), arrayMap(a -> greatest(a, w) + 1, x))
SETTINGS allow_nullable_key = 1, index_granularity = 1;
SYSTEM STOP MERGES t_captured;
INSERT INTO t_captured VALUES ([NULL], 1, 1);
INSERT INTO t_captured VALUES ([5], 1, 2);

SELECT 'key compiled', sum(value) > 0 FROM system.events WHERE event = 'CompileFunction';
SELECT 'baseline projection', count() FROM (
    EXPLAIN SELECT min(arrayCount(z -> isNull(z), arrayMap(a -> greatest(a, w) + 1, x))), max(arrayCount(z -> isNull(z), arrayMap(a -> greatest(a, w) + 1, x)))
    FROM t_captured SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
WHERE explain LIKE '%_minmax_count_projection%';

SET least_greatest_legacy_null_behavior = 0, use_query_condition_cache = 0, optimize_move_to_prewhere = 0;

SELECT 'folded', groupArray(v) FROM (
    SELECT v FROM t_folded WHERE arrayCount(z -> isNull(z), arrayMap((a, b) -> greatest(a, b) + 1, x, y)) = 0
    ORDER BY v SETTINGS optimize_use_implicit_projections = 0);
SELECT 'folded key used', count() FROM (
    EXPLAIN indexes = 1 SELECT v FROM t_folded WHERE arrayCount(z -> isNull(z), arrayMap((a, b) -> greatest(a, b) + 1, x, y)) = 0
    SETTINGS optimize_use_implicit_projections = 0)
WHERE explain ILIKE '%Condition: (arrayCount%';
SELECT 'captured', groupArray(v) FROM (
    SELECT v FROM t_captured WHERE arrayCount(z -> isNull(z), arrayMap(a -> greatest(a, w) + 1, x)) = 0
    ORDER BY v SETTINGS optimize_use_implicit_projections = 0);
SELECT 'deviating projection', count() FROM (
    EXPLAIN SELECT min(arrayCount(z -> isNull(z), arrayMap(a -> greatest(a, w) + 1, x))), max(arrayCount(z -> isNull(z), arrayMap(a -> greatest(a, w) + 1, x)))
    FROM t_captured SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
WHERE explain LIKE '%_minmax_count_projection%';
SELECT 'captured min max', min(arrayCount(z -> isNull(z), arrayMap(a -> greatest(a, w) + 1, x))), max(arrayCount(z -> isNull(z), arrayMap(a -> greatest(a, w) + 1, x)))
FROM t_captured SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
"
