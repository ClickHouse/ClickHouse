#!/usr/bin/env bash

# The query of `view` is sent to parallel replicas as part of a join that each of them runs locally, and the
# replicas do not get the initiator's current database. The tables it reads, including the one named in a table
# function's arguments, must still resolve to the test database's tables there. A `remote` inside it (here in
# an `IN` subquery) is evaluated on the server it names, so its table keeps resolving against that server's
# default database.
# Same-named tables in `default` hold a value the test database's tables do not, so a wrong resolution gives a
# wrong answer rather than an error. Their names carry the test database to stay unique.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

shadow_ts="ts_${CLICKHOUSE_DATABASE}"
shadow_t="t_${CLICKHOUSE_DATABASE}"

CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1"

${CLIENT} -q "
DROP TABLE IF EXISTS default.${shadow_ts};
DROP TABLE IF EXISTS default.${shadow_t};
CREATE TABLE default.${shadow_ts} ENGINE = TimeSeries;
INSERT INTO default.${shadow_ts} (metric_name, tags, samples) VALUES ('m', {'job': 'j'}, [(1, 100.)]);
CREATE TABLE default.${shadow_t} (value Float64) ENGINE = MergeTree ORDER BY value;
INSERT INTO default.${shadow_t} VALUES (100);

CREATE TABLE ${shadow_ts} ENGINE = TimeSeries;
INSERT INTO ${shadow_ts} (metric_name, tags, samples) VALUES ('m', {'job': 'j'}, [(1, 1.)]);
CREATE TABLE ${shadow_t} (value Float64) ENGINE = MergeTree ORDER BY value;
INSERT INTO ${shadow_t} VALUES (1);

CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mt VALUES (1), (100);
"

PR_CLIENT="${CLIENT} --enable_parallel_replicas 1 --max_parallel_replicas 3
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost
    --parallel_replicas_for_non_replicated_merge_tree 1 --automatic_parallel_replicas_mode 0
    --parallel_replicas_prefer_local_join 1 --parallel_replicas_local_plan 0"

join_view() { ${PR_CLIENT} -q "SELECT x FROM mt INNER JOIN view($1) AS r ON mt.x = toUInt64(r.value) ORDER BY x"; }

echo '-- a table'
join_view "SELECT value FROM ${shadow_t}"

echo '-- a table function'
join_view "SELECT samples[1].2 AS value FROM timeSeriesSamples(${shadow_ts})"

echo '-- a nested view'
join_view "SELECT value FROM view(SELECT samples[1].2 AS value FROM timeSeriesSamples(${shadow_ts}))"

echo '-- remote'
${PR_CLIENT} -q "
SELECT x FROM mt
WHERE x IN (SELECT toUInt64(value) FROM view(SELECT samples[1].2 AS value FROM remote('127.0.0.2', timeSeriesSamples(${shadow_ts}))))
ORDER BY x"

${CLIENT} -q "
DROP TABLE default.${shadow_ts};
DROP TABLE default.${shadow_t};
"
