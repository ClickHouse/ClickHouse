#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# The failpoint is process-global (a concurrent test could have its query cancelled or consume it) and needs libfiu.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RESULT_FILE="${CLICKHOUSE_TMP}/05616_row_output_format_cancel_${CLICKHOUSE_DATABASE}"

# Each query writes one block of 100000 rows (`max_block_size` equal to the row count clamps `numbers` to one stream), so only an in-chunk check can leave it early; the failpoint cancels at row 8192.
function check_cancelled
{
    local label="$1"
    local format="$2"
    local extra_url_params="${3:-}"

    ${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT row_output_format_cancel_mid_loop"

    # The server deliberately breaks the HTTP protocol after the exception, so curl may report a transfer error; silence stderr (non-empty stderr fails the test) and rely on the body.
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}${extra_url_params}" -o "$RESULT_FILE" -d "
        SELECT concat('xqzj_', toString(number), repeat('y', 100))
        FROM numbers(100000)
        FORMAT ${format}
        SETTINGS max_block_size = 100000, max_threads = 1, output_format_parallel_formatting = 0" 2>/dev/null || true

    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT row_output_format_cancel_mid_loop"

    if ! grep -qF "QUERY_WAS_CANCELLED" "$RESULT_FILE"; then
        echo "${format}: the cancellation was not reported"
        exit 1
    fi

    if grep -qF "xqzj_99999" "$RESULT_FILE"; then
        echo "${format}: the whole chunk was written before the cancellation"
        exit 1
    fi

    echo "${label}: cancelled"
}

for format in CSV TabSeparated Values Vertical Markdown JSONEachRow; do
    check_cancelled "$format" "$format"
done

# Under `http_write_exception_in_output_format` a separate row loop in `RowOutputFormatWithExceptionHandlerAdaptor` must poll as well.
check_cancelled "JSONEachRow (http_write_exception_in_output_format)" JSONEachRow "&http_write_exception_in_output_format=1"

rm -f "$RESULT_FILE"

