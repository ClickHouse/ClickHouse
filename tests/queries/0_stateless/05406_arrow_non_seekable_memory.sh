#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow format is not available in fasttest builds

# A non-seekable `Arrow` (file) input is loaded into memory. That copy must cost about the file size (130 MiB
# here): it used to grow by doubling to a 384 MiB buffer, which failed these reads.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eo pipefail

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrow.zst"
STDERR_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.stderr"
trap 'rm -f "${DATA_FILE}" "${STDERR_FILE}"' EXIT

GENERATE="SELECT number AS n FROM numbers(17000000)"
CHECK="SELECT count(), sum(n), sum(cityHash64(n))"
# `operator new` does not throw on the limit by default; with `max_untracked_memory = 0` the next allocation, even
# a small one, notices an oversized buffer (schema inference makes only small ones after the copy).
LIMITS="--max_memory_usage 200Mi --max_untracked_memory 0"
# Measured query peaks: 148-159 MiB for max_threads 1-256, 578-582 MiB before the fix.
MAX_PEAK=$((180 << 20))

# Prints the result. Prints stderr and exits with an error if the query fails, or stderr has anything but the peaks
# (one per query, the checked one last), or the last peak is not between 0 and `MAX_PEAK`.
check()
{
    ${CLICKHOUSE_LOCAL} ${LIMITS} --memory-usage "$@" 2>"${STDERR_FILE}" \
        || { cat "${STDERR_FILE}"; echo "The query failed"; exit 1; }
    local peak
    peak=$(tail -n1 "${STDERR_FILE}")
    if grep -qvE '^[0-9]+$' "${STDERR_FILE}" || ! (( peak > 0 && peak < MAX_PEAK )); then
        cat "${STDERR_FILE}"
        echo "Expected only peaks on stderr, the last one between 0 and ${MAX_PEAK}, got '${peak}'"
        exit 1
    fi
}

${CLICKHOUSE_LOCAL} --output_format_arrow_compression_method=none -q "${GENERATE} INTO OUTFILE '${DATA_FILE}' TRUNCATE FORMAT Arrow"

${CLICKHOUSE_LOCAL} --output_format_arrow_compression_method=none -q "${GENERATE} FORMAT Arrow" \
    | check --input-format Arrow --structure 'n UInt64' -q "${CHECK} FROM table"
check -q "${CHECK} FROM file('${DATA_FILE}', Arrow, 'n UInt64')"
check -q "${CHECK} FROM file('${DATA_FILE}', Arrow)"
