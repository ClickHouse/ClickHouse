#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `INSERT ... FROM INFILE` is read by the client. The server must refuse it before it opens the path,
# also for an asynchronous insert: the result must not depend on whether the file exists.

readable="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.txt"
missing="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_missing.txt"
echo "line" > "${readable}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_async_infile (line String) ENGINE = Memory"

for wait in 0 1; do
    for path in "${readable}" "${missing}"; do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=${wait}" \
            --data-binary "INSERT INTO t_async_infile FROM INFILE '${path}' FORMAT LineAsString" \
            | grep -o -m1 'UNKNOWN_TYPE_OF_QUERY\|FILE_DOESNT_EXIST\|CANNOT_OPEN_FILE' || echo "accepted"
    done
done

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_async_infile"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_async_infile"
rm -f "${readable}"
