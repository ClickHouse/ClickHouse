#!/usr/bin/env bash

# The query of `view` sent to parallel replicas as part of a join they run locally names its tables qualified. The
# scalar subqueries in it must still be sent as subqueries, not as the placeholder values of the type-only analysis
# that qualifies it, and the tables they read must be qualified as well, also in a
# constant folded from them by early short-circuit.
# A same-named table in `default` holds a value the test database's table does not, so a wrong resolution gives a
# wrong answer rather than an error. Its name carries the test database to stay unique.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

shadow_ts="ts_${CLICKHOUSE_DATABASE}"

CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1"

${CLIENT} -q "
DROP TABLE IF EXISTS default.${shadow_ts};
CREATE TABLE default.${shadow_ts} ENGINE = TimeSeries;
INSERT INTO default.${shadow_ts} (metric_name, tags, samples) VALUES ('m', {'job': 'j'}, [(1, 100.)]);

CREATE TABLE ${shadow_ts} ENGINE = TimeSeries;
INSERT INTO ${shadow_ts} (metric_name, tags, samples) VALUES ('m', {'job': 'j'}, [(1, 1.)]);

CREATE TABLE source (value UInt64) ENGINE = MergeTree ORDER BY value;
INSERT INTO source VALUES (1);

CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mt VALUES (0), (1), (100);
"

PR_CLIENT="${CLIENT} --enable_parallel_replicas 1 --max_parallel_replicas 3
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost
    --parallel_replicas_for_non_replicated_merge_tree 1 --automatic_parallel_replicas_mode 0
    --parallel_replicas_prefer_local_join 1 --parallel_replicas_local_plan 0"

join_view() { ${PR_CLIENT} "${@:2}" -q "SELECT x FROM mt INNER JOIN view($1) AS r ON mt.x = toUInt64(r.value) ORDER BY x"; }

echo '-- a scalar subquery'
join_view "SELECT (SELECT max(value) FROM source) AS value"

echo '-- an expression of a scalar subquery'
join_view "SELECT (SELECT max(value) FROM source) * 2 - 1 AS value"

echo '-- an array scalar subquery'
join_view "SELECT arrayJoin((SELECT groupArray(value) FROM source)) AS value"

echo '-- a scalar subquery over a table function'
join_view "SELECT (SELECT max(samples[1].2) FROM timeSeriesSamples(${shadow_ts})) AS value"

echo '-- a constant folded by early short-circuit'
join_view "SELECT toUInt64(1 OR (SELECT count() FROM source) > 0) AS value" --enable_function_early_short_circuit 1

echo '-- a constant folded by early short-circuit, in viewIfPermitted'
join_view "SELECT value FROM viewIfPermitted(SELECT assumeNotNull(toUInt64(1 OR (SELECT count() FROM source) > 0)) AS value ELSE null('value UInt64'))" --enable_function_early_short_circuit 1

${CLIENT} -q "DROP TABLE default.${shadow_ts}"
