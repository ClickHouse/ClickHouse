#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `enable_parsing_to_custom_serialization = 1`, input formats parse into the representation given by
# the table-level serialization hints. Once a part of the table is stored with automatic `LowCardinality`
# serialization, every later INSERT is parsed into a `ColumnLowCardinality`, including blocks that are
# stored as plain `String`/`FixedString`: a high-cardinality block, or any block after the threshold is
# lowered to zero. Such inserts must materialize the column instead of throwing an exception.

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_auto_lc_hints
    (
        k UInt64,
        lc String STATISTICS(uniq),
        fs FixedString(4) STATISTICS(uniq),
        INDEX ix fs TYPE bloom_filter
    )
    ENGINE = MergeTree
    ORDER BY (lc, k)
    SETTINGS max_uniq_number_for_low_cardinality = 1000, ratio_of_defaults_for_sparse_serialization = 1, min_bytes_for_wide_part = 0, auto_statistics_types = ''"

${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_auto_lc_hints"

INSERT="${CLICKHOUSE_CLIENT} --enable_parsing_to_custom_serialization 1 --materialize_statistics_on_insert 1 --input_format_parallel_parsing 0 --query"

printf '1\ta\taaaa\n2\tb\tbbbb\n' | ${INSERT} "INSERT INTO t_auto_lc_hints FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "SELECT number + 10, toString(number), leftPad(toString(number % 10000), 4, '0') FROM numbers(20000) FORMAT TSV" \
    | ${INSERT} "INSERT INTO t_auto_lc_hints FORMAT TSV"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_auto_lc_hints MODIFY SETTING max_uniq_number_for_low_cardinality = 0"

printf '3\tc\tcccc\n' | ${INSERT} "INSERT INTO t_auto_lc_hints FORMAT TSV"
printf '4,"d","dddd"\n' | ${INSERT} "INSERT INTO t_auto_lc_hints FORMAT CSV"
printf '{"k":5,"lc":"e","fs":"eeee"}\n' | ${INSERT} "INSERT INTO t_auto_lc_hints FORMAT JSONEachRow"

${CLICKHOUSE_CLIENT} --query "
    SELECT name, column, serialization_kind FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_auto_lc_hints' AND active AND column != 'k'
    ORDER BY name, column"

${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(lc), uniqExact(fs), min(k), max(k) FROM t_auto_lc_hints"
${CLICKHOUSE_CLIENT} --query "SELECT k, lc, fs FROM t_auto_lc_hints WHERE k < 10 ORDER BY k"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES t_auto_lc_hints"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_auto_lc_hints FINAL"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(lc), uniqExact(fs) FROM t_auto_lc_hints"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_auto_lc_hints"
