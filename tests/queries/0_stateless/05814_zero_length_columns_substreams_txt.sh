#!/usr/bin/env bash

# A power loss can leave a file that was never `fsync`ed at length zero while the rest of the part
# survives, and `columns_substreams.txt` is not covered by the part checksums. An absent one is
# handled safely in a Wide part (the streams are enumerated from the serialization instead), but an
# empty one used to fail to parse, which detached the whole part as broken and dropped its rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/05814_zero_length_columns_substreams_txt"

run_case()
{
    local files="$1"
    rm -rf "${WORKING_DIR}"
    mkdir -p "${WORKING_DIR}"

    ${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
        CREATE TABLE t (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
        INSERT INTO t SELECT number, number + 1 FROM numbers(500);
        SELECT 'before ${files}', count(), sum(val) FROM t;
    " </dev/null

    for file_name in ${files}
    do
        find "${WORKING_DIR}" -name "${file_name}" -exec truncate -s 0 {} \;
    done

    # Load twice: the file stays empty on disk, so every load must take the same path.
    for attempt in 1 2
    do
        ${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
            SELECT 'after ${files}, load ${attempt}', count(), sum(val) FROM t;
            SELECT 'detached parts', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't';
        " </dev/null
    done

    rm -rf "${WORKING_DIR}"
}

run_case columns_substreams.txt

# An existing `columns_substreams.txt` means the part may store a column in a layout other than the
# default serialization, so when `columns.txt` is empty as well, its column list cannot be rebuilt
# safely: the part is still detached as broken (and kept in `detached` for recovery), not guessed at.
run_case "columns.txt columns_substreams.txt"
