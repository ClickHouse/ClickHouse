#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo '1';
$CLICKHOUSE_CLIENT --distributed_aggregation_memory_efficient=1 --group_by_two_level_threshold=1 --max_execution_time=1 --query="SELECT SearchPhrase AS k, count() AS c FROM remote('127.0.0.{1,2}', test.hits) GROUP BY k ORDER BY c DESC LIMIT 10" --format=Null 2>/dev/null;
echo '2';
$CLICKHOUSE_CLIENT --distributed_aggregation_memory_efficient=1 --group_by_two_level_threshold=1 --max_execution_time=1 --query="SELECT SearchPhrase AS k, count() AS c FROM remote('127.0.0.{1,2,3}', test.hits) GROUP BY k ORDER BY c DESC LIMIT 10" --format=Null 2>/dev/null;
echo '3';
$CLICKHOUSE_CLIENT --distributed_aggregation_memory_efficient=1 --group_by_two_level_threshold=1 --max_execution_time=1 --query="SELECT SearchPhrase AS k, count() AS c FROM remote('127.0.0.{1,2,3,4}', test.hits) GROUP BY k ORDER BY c DESC LIMIT 10" --format=Null 2>/dev/null;
echo '4';
$CLICKHOUSE_CLIENT --distributed_aggregation_memory_efficient=1 --group_by_two_level_threshold=1 --max_execution_time=1 --query="SELECT SearchPhrase AS k, count() AS c FROM remote('127.0.0.{1,2,3,4,5}', test.hits) GROUP BY k ORDER BY c DESC LIMIT 10" --format=Null 2>/dev/null;
echo '5';
$CLICKHOUSE_CLIENT --distributed_aggregation_memory_efficient=1 --group_by_two_level_threshold=1 --max_execution_time=1 --query="SELECT SearchPhrase AS k, count() AS c FROM remote('127.0.0.{1,2,3,4,5,6}', test.hits) GROUP BY k ORDER BY c DESC LIMIT 10" --format=Null 2>/dev/null;
echo '6';
$CLICKHOUSE_CLIENT --distributed_aggregation_memory_efficient=1 --group_by_two_level_threshold=1 --max_execution_time=1 --query="SELECT SearchPhrase AS k, count() AS c FROM remote('127.0.0.{1,2,3,4,5,6,7}', test.hits) GROUP BY k ORDER BY c DESC LIMIT 10" --format=Null 2>/dev/null;
echo '7';
$CLICKHOUSE_CLIENT --distributed_aggregation_memory_efficient=1 --group_by_two_level_threshold=1 --max_execution_time=1 --query="SELECT SearchPhrase AS k, count() AS c FROM remote('127.0.0.{1,2,3,4,5,6,7,8}', test.hits) GROUP BY k ORDER BY c DESC LIMIT 10" --format=Null 2>/dev/null;
echo '8';
$CLICKHOUSE_CLIENT --distributed_aggregation_memory_efficient=1 --group_by_two_level_threshold=1 --max_execution_time=1 --query="SELECT SearchPhrase AS k, count() AS c FROM remote('127.0.0.{1,2,3,4,5,6,7,8,9}', test.hits) GROUP BY k ORDER BY c DESC LIMIT 10" --format=Null 2>/dev/null;
echo '9';
$CLICKHOUSE_CLIENT --distributed_aggregation_memory_efficient=1 --group_by_two_level_threshold=1 --max_execution_time=1 --query="SELECT SearchPhrase AS k, count() AS c FROM remote('127.0.0.{1,2,3,4,5,6,7,8,9,10}', test.hits) GROUP BY k ORDER BY c DESC LIMIT 10" --format=Null 2>/dev/null;
echo '10';
