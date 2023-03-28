#!/usr/bin/env bash
# Tags: zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="CREATE TABLE landing (time DateTime, number Int64) Engine=ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/landing','{replica}') PARTITION BY toYYYYMMDD(time) ORDER BY time";
$CLICKHOUSE_CLIENT --query="CREATE MATERIALIZED VIEW IF NOT EXISTS mv ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/{database}/mv','{replica}') PARTITION BY toYYYYMMDD(hour) ORDER BY hour AS SELECT toStartOfHour(time) AS hour, sum(number) AS sum_amount FROM landing GROUP BY hour";

printf '2023-09-01 12:23:34, 42,
'| $CLICKHOUSE_CLIENT --query="INSERT INTO landing (*) settings max_insert_delayed_streams_for_parallel_write=0 FORMAT CSV";

printf '2023-09-01 12:23:34, 42,
2022-09-01 12:23:34, 42,
'| $CLICKHOUSE_CLIENT --query="INSERT INTO landing (*) settings max_insert_delayed_streams_for_parallel_write=0 FORMAT CSV";

$CLICKHOUSE_CLIENT --query="""SELECT * FROM landing FINAL ORDER BY time""";
$CLICKHOUSE_CLIENT --query="""SELECT * FROM mv FINAL ORDER BY hour""";


$CLICKHOUSE_CLIENT --query="CREATE TABLE landing2 (time DateTime, number Int64) Engine=ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/landing2','{replica}') PARTITION BY toYYYYMMDD(time) ORDER BY time";
$CLICKHOUSE_CLIENT --query="CREATE MATERIALIZED VIEW IF NOT EXISTS mv2 ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/{database}/mv2','{replica}') PARTITION BY toYYYYMMDD(hour) ORDER BY hour AS SELECT toStartOfHour(time) AS hour, sum(number) AS sum_amount FROM landing2 GROUP BY hour";

printf '2023-09-01 12:23:34, 42,
'| $CLICKHOUSE_CLIENT --query="INSERT INTO landing2 (*) settings max_insert_delayed_streams_for_parallel_write=2 FORMAT CSV";

printf '2023-09-01 12:23:34, 42,
2022-09-01 12:23:34, 42,
'| $CLICKHOUSE_CLIENT --query="INSERT INTO landing2 (*) settings max_insert_delayed_streams_for_parallel_write=2 FORMAT CSV";

$CLICKHOUSE_CLIENT --query="""SELECT * FROM landing2 FINAL ORDER BY time""";
$CLICKHOUSE_CLIENT --query="""SELECT * FROM mv2 FINAL ORDER BY hour""";
