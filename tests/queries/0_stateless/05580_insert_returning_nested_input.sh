#!/usr/bin/env bash
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

${CLICKHOUSE_CLIENT} --query="CREATE TABLE nested_input (x UInt64) ENGINE = Memory"

for source in \
    "loop(input('x UInt64'))" \
    "loop(loop(input('x UInt64')))" \
    "viewIfPermitted(SELECT 1 AS x ELSE input('x UInt64'))" \
    "remote('127.0.0.1', input('x UInt64'))"; do
    if ${CLICKHOUSE_CLIENT} --async_insert=0 --query="
        INSERT INTO nested_input RETURNING (SELECT * FROM ${source} LIMIT 1) SELECT 1;
    " > "${CLICKHOUSE_TMP}/nested_input_error" 2>&1; then
        echo "Expected INVALID_USAGE_OF_INPUT"
        exit 1
    fi
    if ! grep -q INVALID_USAGE_OF_INPUT "${CLICKHOUSE_TMP}/nested_input_error"; then
        cat "${CLICKHOUSE_TMP}/nested_input_error"
        exit 1
    fi
    ${CLICKHOUSE_CLIENT} --query="SELECT count() FROM nested_input"
done

printf '7\n' | ${CLICKHOUSE_CLIENT} --async_insert=0 --query="
    INSERT INTO nested_input SELECT * FROM loop(input('x UInt64')) LIMIT 1 FORMAT TSV"
${CLICKHOUSE_CLIENT} --query="SELECT x FROM nested_input"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE nested_input"

printf '8\n' | ${CLICKHOUSE_CURL} -fsS \
    "${CLICKHOUSE_URL}&async_insert=0&query=INSERT%20INTO%20nested_input%20SELECT%20%2A%20FROM%20loop%28input%28%27x%20UInt64%27%29%29%20LIMIT%201%20FORMAT%20TSV" \
    --data-binary @-
${CLICKHOUSE_CLIENT} --query="SELECT x FROM nested_input"

${CLICKHOUSE_CLIENT} --query="DROP TABLE nested_input"
