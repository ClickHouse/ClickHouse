#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP DATABASE IF EXISTS test SYNC";
$CLICKHOUSE_CLIENT --query="CREATE DATABASE test";
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.landing (time DateTime, number Int64) Engine=ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/test.landing','{replica}') PARTITION BY toYYYYMMDD(time) ORDER BY time";
$CLICKHOUSE_CLIENT --query="CREATE MATERIALIZED VIEW IF NOT EXISTS test.mv ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{layer}-{shard}/test.mv','{replica}') PARTITION BY toYYYYMMDD(hour) ORDER BY hour AS SELECT toStartOfHour(time) AS hour, sum(number) AS sum_amount FROM test.landing GROUP BY hour";

printf '2023-09-01 12:23:34, 42,
'| $CLICKHOUSE_CLIENT --query="INSERT INTO test.landing (*) settings max_insert_delayed_streams_for_parallel_write=0 FORMAT CSV";

printf '2023-09-01 12:23:34, 42,
2022-09-01 12:23:34, 42,
'| $CLICKHOUSE_CLIENT --query="INSERT INTO test.landing (*) settings max_insert_delayed_streams_for_parallel_write=0 FORMAT CSV";

$CLICKHOUSE_CLIENT --query="""SELECT * FROM test.landing FINAL ORDER BY time""";
$CLICKHOUSE_CLIENT --query="""SELECT * FROM test.mv FINAL ORDER BY hour""";


$CLICKHOUSE_CLIENT --query="DROP DATABASE IF EXISTS test SYNC";
$CLICKHOUSE_CLIENT --query="CREATE DATABASE test";
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.landing (time DateTime, number Int64) Engine=ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/test.landing','{replica}') PARTITION BY toYYYYMMDD(time) ORDER BY time";
$CLICKHOUSE_CLIENT --query="CREATE MATERIALIZED VIEW IF NOT EXISTS test.mv ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{layer}-{shard}/test.mv','{replica}') PARTITION BY toYYYYMMDD(hour) ORDER BY hour AS SELECT toStartOfHour(time) AS hour, sum(number) AS sum_amount FROM test.landing GROUP BY hour";

printf '2023-09-01 12:23:34, 42,
'| $CLICKHOUSE_CLIENT --query="INSERT INTO test.landing (*) settings max_insert_delayed_streams_for_parallel_write=2 FORMAT CSV";

printf '2023-09-01 12:23:34, 42,
2022-09-01 12:23:34, 42,
'| $CLICKHOUSE_CLIENT --query="INSERT INTO test.landing (*) settings max_insert_delayed_streams_for_parallel_write=2 FORMAT CSV";

$CLICKHOUSE_CLIENT --query="""SELECT * FROM test.landing FINAL ORDER BY time""";
$CLICKHOUSE_CLIENT --query="""SELECT * FROM test.mv FINAL ORDER BY hour""";
