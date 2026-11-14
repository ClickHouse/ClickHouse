#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The INSERT structure-mismatch diagnostic infers the schema of the failed data again. For `Native`
# the schema reader decodes every column of the block, including a column the failed insert never
# reached. Here the second column declares a 1 GiB string in a few bytes of input without supplying
# it: the diagnostic must not allocate the declared size, and the original error must be kept.

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05627 (u UUID) ENGINE = Memory"

query_id="05627_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"

# Two columns, one row: `u String` = 'not-a-uuid', then `unused String` declaring 2^30 bytes.
printf '\x02\x01\x01u\x06String\x0anot-a-uuid\x06unused\x06String\x80\x80\x80\x80\x04' \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${query_id}&async_insert=0&query=INSERT+INTO+t_05627+FORMAT+Native" --data-binary @- \
    | grep -o -m1 "CANNOT_PARSE_UUID"

# The `query_log` entry is written after the HTTP response is sent, so wait for it.
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    found=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND type = 'ExceptionWhileProcessing'")
    [ "$found" -ge 1 ] && break
    sleep 0.5
done

${CLICKHOUSE_CLIENT} -q "
    SELECT exception_code != 0, memory_usage < 256 * 1024 * 1024
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND type = 'ExceptionWhileProcessing'"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_05627"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_05627"
