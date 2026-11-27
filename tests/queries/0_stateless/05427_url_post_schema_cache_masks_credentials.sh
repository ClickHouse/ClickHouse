#!/usr/bin/env bash
# Tags: no-fasttest
# A URL read with POST is cached as `POST:<url>`; system.schema_inference_cache must still mask its password.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_NAME="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PASSWORD="secret_${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}; CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY '${PASSWORD}'"

URL="http://${USER_NAME}:${PASSWORD}@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1+AS+x+FORMAT+TSVWithNamesAndTypes&param_marker=${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLICKHOUSE_CLIENT -q "SELECT * FROM url('${URL}', 'TSVWithNamesAndTypes', http_method='POST')"

$CLICKHOUSE_CLIENT -q "
    SELECT count(), countIf(position(source, '${PASSWORD}') > 0) FROM system.schema_inference_cache
    WHERE source LIKE 'POST:%' AND position(source, 'param_marker=${CLICKHOUSE_TEST_UNIQUE_NAME}') > 0"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
