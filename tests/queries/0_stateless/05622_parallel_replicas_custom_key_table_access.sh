#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The validation of the custom key for parallel replicas must not reveal a table the user cannot see: it used to
# reject `x IN secret.target` with `BAD_ARGUMENTS` when the table `secret.target` existed, without checking that
# the user had access to it. Now a table that the user cannot see is left to the analyzer, which handles it the
# same way as in an ordinary filter `WHERE x IN secret.target`: with `ACCESS_DENIED` if the table exists and with
# `UNKNOWN_IDENTIFIER` if it does not. A user who can see the table still gets `BAD_ARGUMENTS`.

SECRET_DB="${CLICKHOUSE_DATABASE}_secret"
USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
SETTINGS="parallel_replicas_mode = 'custom_key_sampling', max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_count = 2, parallel_replica_offset = 0, parallel_replicas_custom_key = 'x IN ${SECRET_DB}.target', automatic_parallel_replicas_mode = 0, cluster_for_parallel_replicas = '', parallel_replicas_for_non_replicated_merge_tree = 0, serialize_query_plan = 0, enable_analyzer = 1"
QUERY="SELECT count() FROM ${CLICKHOUSE_DATABASE}.source AS ${SECRET_DB} SETTINGS ${SETTINGS}"
FILTER_QUERY="SELECT count() FROM ${CLICKHOUSE_DATABASE}.source WHERE x IN ${SECRET_DB}.target SETTINGS enable_analyzer = 1"
ERROR_CODE='ACCESS_DENIED\|BAD_ARGUMENTS\|UNKNOWN_IDENTIFIER'

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS ${SECRET_DB};
    DROP USER IF EXISTS ${USER};
    CREATE TABLE ${CLICKHOUSE_DATABASE}.source (x UInt64, target Array(UInt64)) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${CLICKHOUSE_DATABASE}.source SELECT number, [number, 100] FROM numbers(10);
    CREATE DATABASE ${SECRET_DB};
    CREATE TABLE ${SECRET_DB}.target (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE USER ${USER};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.source TO ${USER};
"

echo "--- present"
$CLICKHOUSE_CLIENT --user "${USER}" -q "${QUERY}" 2>&1 | grep -o -m1 "${ERROR_CODE}"
$CLICKHOUSE_CLIENT --user "${USER}" -q "${FILTER_QUERY}" 2>&1 | grep -o -m1 "${ERROR_CODE}"

echo "--- visible"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${SECRET_DB}.target TO ${USER}"
$CLICKHOUSE_CLIENT --user "${USER}" -q "${QUERY}" 2>&1 | grep -o -m1 "${ERROR_CODE}"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT ON ${SECRET_DB}.target FROM ${USER}"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${SECRET_DB}.target"

echo "--- absent"
$CLICKHOUSE_CLIENT --user "${USER}" -q "${QUERY}" 2>&1 | grep -o -m1 "${ERROR_CODE}"
$CLICKHOUSE_CLIENT --user "${USER}" -q "${FILTER_QUERY}" 2>&1 | grep -o -m1 "${ERROR_CODE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER ${USER};
    DROP TABLE ${CLICKHOUSE_DATABASE}.source;
    DROP DATABASE ${SECRET_DB};
"
