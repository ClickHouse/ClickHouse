#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_rf_pages"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

# Pages of 100 rows: the writer closes a page after each batch of `output_format_parquet_batch_size` rows once
# it holds `output_format_parquet_data_page_size` bytes. The format settings of an Iceberg write are the ones
# of the table. Background Iceberg compaction would rewrite the files.
PAGES="allow_experimental_iceberg_compaction = 0, output_format_parquet_batch_size = 100, output_format_parquet_data_page_size = 1"

# `fact`: one data file of 2 row groups (k = 0 .. 799 and 800 .. 1599), page p holding k = p * 100 .. p * 100 + 99,
# so every row group contains a key of `dim` and only the pages can be skipped.
# `fact_q`: one row group, page p holding k = p * 200 .. p * 200 + 99, so 150 is within the bounds of the row
# group but of no page.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --max_insert_threads=1 --query "
CREATE TABLE fact (k Int64, v Int64) ENGINE = IcebergLocal('${ROOT}/fact/', 'Parquet')
SETTINGS ${PAGES}, output_format_parquet_row_group_size = 800;
INSERT INTO fact SELECT number, number FROM numbers(1600);
CREATE TABLE fact_q (k Int64, v Int64) ENGINE = IcebergLocal('${ROOT}/fact_q/', 'Parquet')
SETTINGS ${PAGES}, output_format_parquet_row_group_size = 100000;
INSERT INTO fact_q SELECT number + 100 * intDiv(number, 100), number FROM numbers(1600);
CREATE TABLE dim (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dim VALUES (705), (1510);
CREATE TABLE dim_q (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dim_q VALUES (150);
" < /dev/null

# Every setting that decides whether the probe side gets a runtime filter, which side is probed, or whether row
# groups and pages are pruned by statistics is pinned.
SETTINGS="enable_join_runtime_filters = 1, join_algorithm = 'hash', enable_join_runtime_filters_index_analysis = 1, use_iceberg_partition_pruning = 1,
    join_runtime_filter_min_probe_rows = 0, join_runtime_filter_exact_values_limit = 10000, query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
    use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, use_iceberg_metadata_files_cache = 0,
    input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1, use_query_condition_cache = 0"

# Runs the query with the pruning on and off: both must return the same rows.
probe() { # probe <tag> <query> [<extra settings>]
    local extra="${3:+, $3}, max_threads = 1"
    ${CLICKHOUSE_CLIENT} --query "
        $2 SETTINGS ${SETTINGS}${extra}, use_join_runtime_filters_for_iceberg_pruning = 1, log_comment = '${CLICKHOUSE_DATABASE}_$1_on';
        $2 SETTINGS ${SETTINGS}${extra}, use_join_runtime_filters_for_iceberg_pruning = 0, log_comment = '${CLICKHOUSE_DATABASE}_$1_off';"
}

# The pages and row groups the probe pruned by the runtime filter, and all pages it pruned, with the pruning on and off.
report() { # report <tag>
    ${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_', ''), ProfileEvents['ParquetRuntimeFilterPrunedPages'],
            ProfileEvents['ParquetRuntimeFilterPrunedRowGroups'], ProfileEvents['ParquetPrunedPages']
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND log_comment IN ('${CLICKHOUSE_DATABASE}_$1_on', '${CLICKHOUSE_DATABASE}_$1_off')
        ORDER BY log_comment DESC
        SETTINGS enable_parallel_replicas = 0"
}

echo '--- A an exact set of keys ---'
probe a "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.k = d.k"
report a

# The filter of the query skips page 0 of k and pages 10 to 15 of v, which are not counted as pruned by the runtime filter.
echo '--- W together with a filter of the query ---'
probe w "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.k = d.k WHERE f.k >= 100 AND f.v < 1000"
report w

# The runtime filter skips every page of the only row group, which still matches `v >= 0`, so the query condition
# cache must not learn from this read: the plain query after it reads the whole table.
echo '--- Q1 the query condition cache ---'
probe q1 "SELECT count(), sum(v) FROM fact_q AS f INNER JOIN dim_q AS d ON f.k = d.k WHERE f.v >= 0" \
    "optimize_move_to_prewhere = 0, use_query_condition_cache = 1"
report q1
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM fact_q WHERE v >= 0 SETTINGS use_query_condition_cache = 1, use_iceberg_metadata_files_cache = 0"

# The page index of the join key cannot be parsed: the runtime filter skips no page of it, and the read does not fail.
echo '--- M a page index of the join key that cannot be parsed ---'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --max_insert_threads=1 --query "
CREATE TABLE fact_m (k Int64, v Int64) ENGINE = IcebergLocal('${ROOT}/fact_m/', 'Parquet')
SETTINGS ${PAGES}, output_format_parquet_row_group_size = 100000;
INSERT INTO fact_m SELECT number, number FROM numbers(1600);
" < /dev/null
python3 - "${ROOT}"/fact_m/data/*.parquet <<'PYEOF'
import struct, sys
assert len(sys.argv) == 2, sys.argv
buf = bytearray(open(sys.argv[1], "rb").read())

def varint(pos):
    r = s = 0
    while True:
        b = buf[pos]; pos += 1; r |= (b & 0x7F) << s; s += 7
        if not b & 0x80: return r, pos

def walk(pos, ct, path, out):
    """Skips one Thrift compact protocol value of type `ct`; out[path] = (value start, end, int value)."""
    start, val = pos, None
    if ct == 3: pos += 1
    elif ct in (4, 5, 6): u, pos = varint(pos); val = (u >> 1) ^ -(u & 1)
    elif ct == 7: pos += 8
    elif ct == 8: n, pos = varint(pos); pos += n
    elif ct in (9, 10):
        h = buf[pos]; pos += 1; n = h >> 4
        if n == 15: n, pos = varint(pos)
        for i in range(n): pos = pos + 1 if h & 15 in (1, 2) else walk(pos, h & 15, path + (i,), out)
    elif ct == 12:
        fid = 0
        while buf[pos]:
            h = buf[pos]; pos += 1
            if h >> 4: fid += h >> 4
            else: u, pos = varint(pos); fid = (u >> 1) ^ -(u & 1)
            pos = walk(pos, h & 15, path + (fid,), out)
        pos += 1
    elif ct not in (1, 2): raise ValueError(ct)
    out[path] = (start, pos, val)
    return pos

meta = {}
walk(len(buf) - 8 - struct.unpack("<I", buf[-8:-4])[0], 12, (), meta)
# FileMetaData.row_groups[0].columns[0] (`k`): column_index_offset 6, column_index_length 7.
offset, length = meta[(4, 0, 1, 0, 6)][2], meta[(4, 0, 1, 0, 7)][2]
buf[offset:offset + length] = b"\xff" * length   # same size: the manifest's file size still matches
open(sys.argv[1], "wb").write(buf)
PYEOF
probe m "SELECT count(), sum(v) FROM fact_m AS f INNER JOIN dim AS d ON f.k = d.k"
report m

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS fact SYNC;
    DROP TABLE IF EXISTS fact_q SYNC;
    DROP TABLE IF EXISTS fact_m SYNC;
    DROP TABLE IF EXISTS dim SYNC;
    DROP TABLE IF EXISTS dim_q SYNC;"
