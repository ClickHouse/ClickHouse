#!/usr/bin/env bash
# The size columns of system.projections used to crash when a projection was
# present in metadata but missing from an active part, which a stress run hit
# while merges and ALTER MATERIALIZE ran alongside the read (#70106, reverted
# by #70858). Keep a reader on those columns across that window.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS proj_sizes"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE proj_sizes
    (
        key UInt64,
        value String
    )
    ENGINE = MergeTree
    ORDER BY key
"

stop_flag=$(mktemp)
rm -f "${stop_flag}"

(
    for _ in $(seq 1 200); do
        if [[ -f "${stop_flag}" ]]; then
            exit 0
        fi
        ${CLICKHOUSE_CLIENT} --query "
            SELECT total_rows, data_compressed_bytes, data_uncompressed_bytes
            FROM system.projections
            WHERE database = currentDatabase() AND table = 'proj_sizes'
            FORMAT Null
        "
    done
) &
reader_pid=$!

${CLICKHOUSE_CLIENT} --query "INSERT INTO proj_sizes SELECT number, toString(number) FROM numbers(10000)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE proj_sizes ADD PROJECTION p (SELECT * ORDER BY value, key)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE proj_sizes MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2"

for _ in $(seq 1 8); do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO proj_sizes SELECT number, toString(number) FROM numbers(1000)"
    ${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE proj_sizes"
done

touch "${stop_flag}"
wait "${reader_pid}"

${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE proj_sizes FINAL"

${CLICKHOUSE_CLIENT} --query "
    SELECT throwIf(
        (SELECT total_rows FROM system.projections WHERE database = currentDatabase() AND table = 'proj_sizes' AND name = 'p')
        != (SELECT count() FROM proj_sizes))
    FORMAT Null
"

${CLICKHOUSE_CLIENT} --query "
    SELECT throwIf(
        (SELECT total_rows FROM system.projections WHERE database = currentDatabase() AND table = 'proj_sizes' AND name = 'p')
        != (
            SELECT ifNull(sum(rows), 0)
            FROM system.projection_parts
            WHERE database = currentDatabase() AND table = 'proj_sizes' AND name = 'p' AND active
        ))
    FORMAT Null
"

${CLICKHOUSE_CLIENT} --query "
    SELECT throwIf(
        (SELECT data_compressed_bytes FROM system.projections WHERE database = currentDatabase() AND table = 'proj_sizes' AND name = 'p')
        != (
            SELECT ifNull(sum(data_compressed_bytes), 0)
            FROM system.projection_parts
            WHERE database = currentDatabase() AND table = 'proj_sizes' AND name = 'p' AND active
        ))
    FORMAT Null
"

${CLICKHOUSE_CLIENT} --query "
    SELECT throwIf(
        (SELECT data_uncompressed_bytes FROM system.projections WHERE database = currentDatabase() AND table = 'proj_sizes' AND name = 'p')
        != (
            SELECT ifNull(sum(data_uncompressed_bytes), 0)
            FROM system.projection_parts
            WHERE database = currentDatabase() AND table = 'proj_sizes' AND name = 'p' AND active
        ))
    FORMAT Null
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE proj_sizes"
rm -f "${stop_flag}"
