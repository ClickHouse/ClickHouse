#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# The query sent to parallel replicas qualifies the table named by a table function's arguments with the
# initiator's current database. A table function nested in `remote` is evaluated on the server that `remote`
# names, so its table must keep resolving against that server's default database, `default`. A same-named
# table there holds a value the test database's table does not, so a wrong qualification gives a wrong answer
# rather than an error. Its name carries the test database to stay unique.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

shadow="ts_${CLICKHOUSE_DATABASE}"

CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1"

${CLIENT} -q "
DROP TABLE IF EXISTS default.${shadow};
CREATE TABLE default.${shadow} ENGINE = TimeSeries;
INSERT INTO default.${shadow} (metric_name, tags, samples) VALUES ('m', {'job': 'j'}, [(1, 100.)]);

CREATE TABLE ${shadow} ENGINE = TimeSeries;
INSERT INTO ${shadow} (metric_name, tags, samples) VALUES ('m', {'job': 'j'}, [(1, 1.)]);

CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mt VALUES (1), (100);

CREATE TABLE dst (x UInt64) ENGINE = Null;
CREATE TABLE res (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW dst_mv TO res AS SELECT x FROM dst;
"

PR_CLIENT="${CLIENT} --enable_parallel_replicas 1 --max_parallel_replicas 3
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost
    --parallel_replicas_for_non_replicated_merge_tree 1 --automatic_parallel_replicas_mode 0"

remote_in="x IN (SELECT toUInt64(value) FROM remote('127.0.0.2', timeSeriesSamples(${shadow})))"

echo '-- remote, local plan'
${PR_CLIENT} --parallel_replicas_local_plan 1 -q "SELECT x FROM mt WHERE ${remote_in} ORDER BY x"

echo '-- remote, no local plan'
${PR_CLIENT} --parallel_replicas_local_plan 0 -q "SELECT x FROM mt WHERE ${remote_in} ORDER BY x"

echo '-- remote, custom key'
${PR_CLIENT} --parallel_replicas_mode custom_key_sampling --parallel_replicas_custom_key x \
    -q "SELECT x FROM mt WHERE ${remote_in} ORDER BY x"

echo '-- remote, insert select'
${PR_CLIENT} --parallel_replicas_local_plan 0 --parallel_distributed_insert_select 2 \
    -q "INSERT INTO dst SELECT x FROM mt WHERE ${remote_in}"
${CLIENT} -q "SELECT x FROM res ORDER BY x"

# A table function evaluated by each replica itself still names the test database's table.
echo '-- loop'
${PR_CLIENT} --parallel_replicas_local_plan 0 -q "
SELECT x FROM mt WHERE x IN (SELECT toUInt64(value) FROM (SELECT value FROM loop(timeSeriesSamples(${shadow})) LIMIT 1))
ORDER BY x"

${CLIENT} -q "DROP TABLE default.${shadow}"
