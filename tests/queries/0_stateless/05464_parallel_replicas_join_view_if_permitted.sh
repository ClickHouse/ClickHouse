#!/usr/bin/env bash

# The query of `viewIfPermitted` sent to parallel replicas as part of a join they run locally must resolve to the
# test database's tables there, as the query of `view` does. The replicas must read the branch the initiator chose,
# whatever user they run as and whatever they may read in their own default database: a user who may see the
# columns of a table but not read it gets the `ELSE` table function, and a `view` or `viewIfPermitted` there reads
# the test database's tables as well.
# Same-named tables in `default` hold a value the test database's tables do not, so a wrong resolution gives a
# wrong answer rather than an error. Their names carry the test database to stay unique.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

shadow_ts="ts_${CLICKHOUSE_DATABASE}"
shadow_t="t_${CLICKHOUSE_DATABASE}"
shadow_t2="t2_${CLICKHOUSE_DATABASE}"
user="u_${CLICKHOUSE_DATABASE}"

CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1"

${CLIENT} -q "
DROP TABLE IF EXISTS default.${shadow_ts};
DROP TABLE IF EXISTS default.${shadow_t};
DROP TABLE IF EXISTS default.${shadow_t2};
CREATE TABLE default.${shadow_ts} ENGINE = TimeSeries;
INSERT INTO default.${shadow_ts} (metric_name, tags, samples) VALUES ('m', {'job': 'j'}, [(1, 100.)]);
CREATE TABLE default.${shadow_t} (value UInt64) ENGINE = MergeTree ORDER BY value;
INSERT INTO default.${shadow_t} VALUES (100);
CREATE TABLE default.${shadow_t2} (value UInt64) ENGINE = MergeTree ORDER BY value;
INSERT INTO default.${shadow_t2} VALUES (100);

CREATE TABLE ${shadow_ts} ENGINE = TimeSeries;
INSERT INTO ${shadow_ts} (metric_name, tags, samples) VALUES ('m', {'job': 'j'}, [(1, 1.)]);
CREATE TABLE ${shadow_t} (value UInt64) ENGINE = MergeTree ORDER BY value;
INSERT INTO ${shadow_t} VALUES (1);
CREATE TABLE ${shadow_t2} (value UInt64) ENGINE = MergeTree ORDER BY value;
INSERT INTO ${shadow_t2} VALUES (1);

CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mt VALUES (1), (100);

DROP USER IF EXISTS ${user};
CREATE USER ${user};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.mt TO ${user};
GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.${shadow_t} TO ${user};
GRANT SELECT ON default.${shadow_t} TO ${user};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${shadow_t2} TO ${user};
GRANT SELECT ON default.${shadow_t2} TO ${user};
GRANT REMOTE, CREATE TEMPORARY TABLE ON *.* TO ${user};
"

PR_CLIENT="${CLIENT} --enable_parallel_replicas 1 --max_parallel_replicas 3
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost
    --parallel_replicas_for_non_replicated_merge_tree 1 --automatic_parallel_replicas_mode 0
    --parallel_replicas_prefer_local_join 1 --parallel_replicas_local_plan 0 --log_comment 05464_join"

join_view() { ${PR_CLIENT} "${@:2}" -q "SELECT x FROM mt INNER JOIN $1 AS r ON mt.x = r.value ORDER BY x"; }

echo '-- a table'
join_view "viewIfPermitted(SELECT value FROM ${shadow_t} ELSE null('value UInt64'))"

echo '-- a table function'
join_view "viewIfPermitted(SELECT toUInt64(samples[1].2) AS value FROM timeSeriesSamples(${shadow_ts}) ELSE null('value UInt64'))"

echo '-- inside view'
join_view "view(SELECT value FROM viewIfPermitted(SELECT value FROM ${shadow_t} ELSE null('value UInt64')))"

echo '-- not permitted'
join_view "viewIfPermitted(SELECT value FROM ${shadow_t} ELSE null('value UInt64'))" --user "${user}"

echo '-- not permitted, inside view'
join_view "view(SELECT value FROM viewIfPermitted(SELECT value FROM ${shadow_t} ELSE null('value UInt64')))" --user "${user}"

echo '-- not permitted, ELSE view'
join_view "viewIfPermitted(SELECT value FROM ${shadow_t} ELSE view(SELECT value FROM ${shadow_t2}))" --user "${user}"

echo '-- not permitted, ELSE viewIfPermitted'
join_view "viewIfPermitted(SELECT value FROM ${shadow_t} ELSE viewIfPermitted(SELECT value FROM ${shadow_t2} ELSE null('value UInt64')))" --user "${user}"

echo '-- the replicas do not choose a branch'
${CLIENT} -q "
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0, countIf(query LIKE '%viewIfPermitted%')
FROM system.query_log
WHERE type = 'QueryFinish' AND NOT is_initial_query AND event_date >= yesterday()
  AND initial_query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
      AND log_comment = '05464_join' AND event_date >= yesterday());
"

${CLIENT} -q "
DROP USER ${user};
DROP TABLE default.${shadow_ts};
DROP TABLE default.${shadow_t};
DROP TABLE default.${shadow_t2};
"
