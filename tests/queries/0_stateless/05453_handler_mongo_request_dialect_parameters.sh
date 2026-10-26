#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The stored text of a handler is parsed as ClickHouse SQL whatever `dialect` the request names, and
# so its query parameters are substituted as for any other SQL - also when the request names the
# `mongo` dialect, whose own statements are not searched for query parameters. See
# `05052_handler_request_dialect` for the other dialects.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique name and URL so parallel tests do not interfere (handlers are a global namespace).
HANDLER="h_mongo_dialect_${CLICKHOUSE_DATABASE}"
URL="/mongo_dialect_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${HANDLER}"
${CLICKHOUSE_CLIENT} --query "CREATE HANDLER ${HANDLER} URL '${URL}' AS SELECT 'stored' AS s, {x:UInt64} AS p"

${CLICKHOUSE_CURL} -sS "${BASE}${URL}?dialect=mongo&param_x=7"
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?dialect=mongo&allow_experimental_mongo_dialect=1&param_x=8"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${HANDLER}"
