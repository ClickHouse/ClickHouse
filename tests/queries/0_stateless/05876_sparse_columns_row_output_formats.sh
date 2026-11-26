#!/usr/bin/env bash
# Row output formats write sparse columns (with and without values, Nullable, a long default, several chunks) as full columns.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t_sparse_rows;
CREATE TABLE t_sparse_rows (id UInt32, u UInt64, s String, f Float64, d Decimal(9, 2), n Nullable(String), e UInt8, fs FixedString(40), t Tuple(a UInt64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, nullable_serialization_version = 'allow_sparse', serialization_info_version = 'with_types', min_bytes_for_wide_part = 0;
INSERT INTO t_sparse_rows SELECT
    number,
    if(number IN (0, 5, 11), number * 1000 + 7, 0),
    multiIf(number = 2, 'a\tb\nc', number = 3, 'q\"\\'\\\\<&>|', ''),
    if(number = 4, 1.5, 0),
    if(number = 7, -3.25, 0),
    if(number IN (1, 8), concat('n', toString(number)), NULL),
    0,
    if(number = 6, 'abc', ''),
    (if(number = 9, 42, 0), if(number = 10, 'x', ''))
FROM numbers(12);
SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sparse_rows' AND active ORDER BY column;
"

SETTINGS="SETTINGS allow_special_serialization_kinds_in_output_formats = 1, output_format_trim_fixed_string = 0"

for f in TSV TSVRaw CSV JSONEachRow JSONStringsEachRow Values; do
    echo "$f"
    $CLICKHOUSE_CLIENT --allow_special_serialization_kinds_in_output_formats 1 -q "SELECT * FROM t_sparse_rows ORDER BY id FORMAT $f $SETTINGS" | tr '\0' '@'
    if [ "$f" = Values ]; then echo; fi
done

for f in XML JSON; do
    echo "$f"
    $CLICKHOUSE_CLIENT --allow_special_serialization_kinds_in_output_formats 1 -q "SELECT id, u, s, n FROM t_sparse_rows WHERE id < 6 ORDER BY id FORMAT $f $SETTINGS, output_format_write_statistics = 0" | tr '\0' '@'
done

echo "Markdown"
$CLICKHOUSE_CLIENT --allow_special_serialization_kinds_in_output_formats 1 -q "SELECT id, u, s, n, fs FROM t_sparse_rows WHERE id IN (0, 1, 3, 6) ORDER BY id FORMAT Markdown $SETTINGS, output_format_markdown_escape_special_characters = 1" | tr '\0' '@'

echo "RowBinary"
$CLICKHOUSE_CLIENT --allow_special_serialization_kinds_in_output_formats 1 -q "SELECT * FROM t_sparse_rows ORDER BY id FORMAT RowBinary $SETTINGS" \
    | $CLICKHOUSE_LOCAL --input-format RowBinary --structure 'id UInt32, u UInt64, s String, f Float64, d Decimal(9, 2), n Nullable(String), e UInt8, fs FixedString(40), t Tuple(a UInt64, b String)' \
        -q "SELECT * FROM table FORMAT TSV SETTINGS output_format_trim_fixed_string = 0" | tr '\0' '@'

echo "TSV, several chunks"
$CLICKHOUSE_CLIENT --allow_special_serialization_kinds_in_output_formats 1 -q "SELECT * FROM t_sparse_rows ORDER BY id FORMAT TSV $SETTINGS, max_block_size = 5, format_tsv_null_representation = '<NULL>'" | tr '\0' '@'

$CLICKHOUSE_CLIENT -q "DROP TABLE t_sparse_rows"
