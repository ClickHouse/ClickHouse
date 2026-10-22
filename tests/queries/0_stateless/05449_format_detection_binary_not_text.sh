#!/usr/bin/env bash
# Binary data must not be detected as `TSV` or `CSV`, which accept almost any bytes: a file whose data has a zero byte
# and that no binary format reads fails with `CANNOT_DETECT_FORMAT`. Text in an unknown file is still detected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.dbf"
TEXT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.data"
trap 'rm -f "${DATA_FILE}" "${TEXT_FILE}"' EXIT

# The start of a dBase table, as in the `.dbf` file of a shapefile: a header, a field descriptor and one record.
printf '\x03\x7a\x0a\x16\x01\x00\x00\x00\x41\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00NAME\x00\x00\x00\x00\x00\x00\x00C\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0d foo       \x1a' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "DESC file('${DATA_FILE}')" 2>&1 | grep -o -F 'CANNOT_DETECT_FORMAT' | head -1

printf 'a\t1\nb\t2\n' > "${TEXT_FILE}"
${CLICKHOUSE_LOCAL} --query "DESC file('${TEXT_FILE}')"
