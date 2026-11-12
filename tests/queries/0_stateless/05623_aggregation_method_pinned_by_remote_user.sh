#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two-level aggregation numbers its buckets by the hash of the method of the key, and the initiator of a
# distributed query merges the buckets of all servers by their numbers. The initiator passes the settings that
# choose the method, `enable_packed_string_keys_in_aggregation` and
# `enable_fixed_and_packed_string_keys_in_aggregation`, to the remote servers. A remote server whose settings
# constraints keep another value disables two-level aggregation for the query: it sends single-level blocks,
# and the initiator splits them into buckets with its own method.
#
# The shard 127.0.0.1 runs in the process of the initiator, with its settings. The shard 127.0.0.2 runs as a
# user whose profile pins both settings to 0 as `READONLY`. The strings take 12 bytes, a length at which the
# packed and the legacy methods of a single `String` key put a key into different buckets. The two methods of
# fixed-width keys and one `String` key do so at any length. Each of the first two queries enables one of the
# settings, which the remote user cannot follow, and passes the pinned value of the other; the third one
# passes the pinned values of both. A key split between two buckets would be counted twice, so a query would
# return about 8000 groups instead of 4000. The last lines show that the remote server aggregates in one level
# only when its constraints changed a value.

user="user_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${profile}"
${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE ${profile} SETTINGS
    enable_packed_string_keys_in_aggregation = 0 READONLY, enable_fixed_and_packed_string_keys_in_aggregation = 0 READONLY"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} SETTINGS PROFILE '${profile}'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t (k UInt32, s String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t SELECT number % 4000, leftPad(toString(number % 4000), 12, '0') FROM numbers(100000)"

# Prints the label `$2` and the groups and rows of `GROUP BY $3` over both shards, under the settings of the
# methods `$4`. The tables convert to two levels at the first key, and `max_bytes_before_external_group_by`
# lets the aggregation of a single stream convert too.
function check()
{
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_DATABASE}_$1" --query "
        SELECT '$2', count(), sum(c)
        FROM (SELECT $3, count() AS c FROM remote('127.0.0.{1,2}:${CLICKHOUSE_PORT_TCP}', currentDatabase(), t, '${user}') GROUP BY $3)
        SETTINGS $4, log_comment = '$2', prefer_localhost_replica = 1, serialize_query_plan = 0,
            distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
            max_bytes_before_external_group_by = 1000000000, max_bytes_ratio_before_external_group_by = 0"
}

check 1 "single String key" "s" \
    "enable_packed_string_keys_in_aggregation = 1, enable_fixed_and_packed_string_keys_in_aggregation = 0"
check 2 "fixed-width keys and a String key" "k, s" \
    "enable_packed_string_keys_in_aggregation = 0, enable_fixed_and_packed_string_keys_in_aggregation = 1"
check 3 "settings agree" "k, s" \
    "enable_packed_string_keys_in_aggregation = 0, enable_fixed_and_packed_string_keys_in_aggregation = 0"

# Whether the aggregations of the initiator and of the remote server converted their tables to two levels.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT any(log_comment),
        countIf(is_initial_query AND ProfileEvents['AggregationConvertedToTwoLevel'] > 0) > 0,
        countIf(NOT is_initial_query AND ProfileEvents['AggregationConvertedToTwoLevel'] > 0) > 0
    FROM system.query_log
    WHERE type = 'QueryFinish' AND query_kind = 'Select'
        AND initial_query_id IN ('${CLICKHOUSE_DATABASE}_1', '${CLICKHOUSE_DATABASE}_2', '${CLICKHOUSE_DATABASE}_3')
    GROUP BY initial_query_id
    ORDER BY initial_query_id"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE ${profile}"
