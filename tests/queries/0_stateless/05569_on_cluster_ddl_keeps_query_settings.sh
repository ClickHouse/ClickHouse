#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: ON CLUSTER is not allowed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query stored by an ON CLUSTER statement keeps the settings written in it, exactly as without ON CLUSTER.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
# Handlers are a global namespace.
H_FILTER="h_on_cluster_filter_${CLICKHOUSE_DATABASE}"
H_FORMAT="h_on_cluster_format_${CLICKHOUSE_DATABASE}"
URL_FILTER="/on_cluster_filter_${CLICKHOUSE_DATABASE}"
URL_FORMAT="/on_cluster_format_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${H_FILTER}; DROP HANDLER IF EXISTS ${H_FORMAT}"
}
trap cleanup EXIT
cleanup

DDL="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none"

echo "materialized view"
${DDL} --query "
    CREATE TABLE src (number UInt64) ENGINE = MergeTree ORDER BY number;
    CREATE MATERIALIZED VIEW mv ON CLUSTER test_shard_localhost ENGINE = MergeTree ORDER BY number
        AS SELECT number FROM src SETTINGS additional_result_filter = 'number % 2 = 0';
    CREATE MATERIALIZED VIEW rmv REFRESH EVERY 1 YEAR APPEND TO src EMPTY AS SELECT number + 10 AS number FROM numbers(4);
    INSERT INTO src SELECT number FROM numbers(6);
    SYSTEM REFRESH VIEW rmv;
    SYSTEM WAIT VIEW rmv;
    SELECT groupArray(number) FROM (SELECT number FROM mv ORDER BY number);
"

echo "modify query"
${DDL} --query "
    CREATE TABLE src2 (number UInt64) ENGINE = MergeTree ORDER BY number;
    CREATE MATERIALIZED VIEW mq ENGINE = MergeTree ORDER BY number AS SELECT number FROM src2;
    ALTER TABLE mq ON CLUSTER test_shard_localhost MODIFY QUERY SELECT number FROM src2 SETTINGS additional_result_filter = 'number % 3 = 0';
    INSERT INTO src2 SELECT number FROM numbers(6);
    SELECT groupArray(number) FROM (SELECT number FROM mq ORDER BY number);
"
${DDL} --query "ALTER TABLE mq ON CLUSTER test_shard_localhost MODIFY QUERY SELECT number FROM src2 SETTINGS limit = 1" 2>&1 \
    | grep -o 'NOT_IMPLEMENTED' | sort -u

echo "handler"
${DDL} --query "
    CREATE HANDLER ${H_FILTER} ON CLUSTER test_shard_localhost URL '${URL_FILTER}'
        AS SELECT number FROM numbers(3) SETTINGS additional_result_filter = 'number = 1';
    CREATE HANDLER ${H_FORMAT} ON CLUSTER test_shard_localhost URL '${URL_FORMAT}'
        AS SELECT number FROM numbers(2) SETTINGS default_format = 'JSONEachRow';
"
${CLICKHOUSE_CURL} -sS "${BASE}${URL_FILTER}"
${CLICKHOUSE_CURL} -sS "${BASE}${URL_FORMAT}"
${DDL} --query "
    ALTER HANDLER ${H_FORMAT} ON CLUSTER test_shard_localhost
        AS SELECT number FROM numbers(2) SETTINGS default_format = 'JSONCompactEachRow';
"
${CLICKHOUSE_CURL} -sS "${BASE}${URL_FORMAT}"
