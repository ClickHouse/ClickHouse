#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `ArrowStream` format needs Arrow, which is not built in the fast test.

# A struct field of an `ArrowStream` file requested both as a whole column and as a subcolumn, with types
# that read its raw bytes differently (`IPv6`, a big integer, `String`), must be decoded consistently for both.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The queries run in a few `clickhouse-local` processes rather than in one per query, which is too slow in the
# sanitizer builds. Each file is written with `INTO OUTFILE` into a directory of its own, under a relative name.
dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$dir"
mkdir -p "$dir"
cd "$dir" || exit 1

$CLICKHOUSE_LOCAL <<'EOF'
SELECT CAST(tuple(false, 'abcdefghijklmnop') AS Tuple(c0 Bool, s String)) AS t FROM numbers(3) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;

SELECT * FROM file('data.arrows', 'ArrowStream', 't String, `t.s` IPv6');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(c0 Bool), `t.s` IPv6');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(c0 Bool, s String), `t.s` IPv6') LIMIT 1;
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.s` IPv6') LIMIT 1;

SELECT 'reverse direction';
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(c0 Bool, s IPv6), `t.s` String') LIMIT 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(c0 Bool, s IPv6), `t.s` UInt128') LIMIT 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(c0 Bool, s IPv6), `t.s` IPv6') LIMIT 1;

SELECT 'dictionary';
SELECT CAST(tuple(false, 'abcdefghijklmnop') AS Tuple(c0 Bool, s LowCardinality(String))) AS t FROM numbers(3) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream SETTINGS output_format_arrow_low_cardinality_as_dictionary = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't String, `t.s` IPv6') LIMIT 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(c0 Bool, s IPv6), `t.s` String') LIMIT 1;

SELECT 'nested';
SELECT CAST(tuple(tuple('abcdefghijklmnop')) AS Tuple(u Tuple(s String))) AS t FROM numbers(3) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(u Tuple(s IPv6)), `t.u` String') LIMIT 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't String, `t.u` Tuple(s IPv6)') LIMIT 1;
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.u` Tuple(s IPv6), `t.u.s` String') LIMIT 1;

SELECT 'date range of a subcolumn';
SELECT CAST(tuple(false, toDate32('1969-12-31')) AS Tuple(c0 Bool, d Date32)) AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
EOF

$CLICKHOUSE_LOCAL -q "SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(c0 Bool), \`t.d\` Date') SETTINGS date_time_overflow_behavior = 'throw'" 2>&1 | grep -o -m1 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'

$CLICKHOUSE_LOCAL <<'EOF'
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(c0 Bool), `t.d` Date') SETTINGS date_time_overflow_behavior = 'saturate';

SELECT 'dictionary of 16-byte values only';
EOF

# Written by PyArrow: a struct child encoded with a dictionary that holds only `abcdefghijklmnop`, so the
# dictionary-wide width check does not decline the conversion (the ClickHouse writer adds an empty entry).
echo "//////AAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAEAAAAkP///wQAAAABAAAABAAAAHj///8AAAENGAAAABwAAAAEAAAAAgAAAIAAAAAgAAAAAQAAAHQAAABw////EAAYAAgABgAHAAwAEAAUABAAAAAAAAEEFAAAADwAAAAcAAAABAAAAAAAAAABAAAAcwAAAAgACAAAAAQACAAAAAwAAAAIAAwACAAHAAgAAAAAAAABIAAAAMz///8QABQACAAGAAcADAAAABAAEAAAAAAAAQYQAAAAGAAAAAQAAAAAAAAAAgAAAGMwAAAEAAQABAAAAAAAAAD/////qAAAABQAAAAAAAAADAAUAAYABQAIAAwADAAAAAACBAAUAAAAGAAAAAAAAAAIAAoAAAAEAAgAAAAQAAAAAAAKABgADAAEAAgACgAAAEwAAAAQAAAAAQAAAAAAAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAAAAAAAAQAAAAAAAAAAAAAAABAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAYWJjZGVmZ2hpamtsbW5vcP/////YAAAAFAAAAAAAAAAMABYABgAFAAgADAAMAAAAAAMEABgAAAAYAAAAAAAAAAAACgAYAAwABAAIAAoAAABsAAAAEAAAAAMAAAAAAAAAAAAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAMAAAAAAAAAAAAAAADAAAAAwAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/////wAAAAA=" | base64 -d > data.arrows

$CLICKHOUSE_LOCAL <<'EOF'
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(c0 Bool, s IPv6), `t.s` String') LIMIT 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't String, `t.s` IPv6') LIMIT 1;

SELECT 'map keys and values';
SELECT map('abcdefghijklmnop', 1) AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(IPv6, UInt8), `t.keys` Array(String)');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(String, UInt8), `t.keys` Array(IPv6)');
SELECT map(1, 'abcdefghijklmnop') AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(UInt8, IPv6), `t.values` Array(String)');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(UInt8, String), `t.values` Array(IPv6)');

SELECT 'nested map keys and values';
SELECT CAST(map(tuple('abcdefghijklmnop'), 1) AS Map(Tuple(s String), UInt8)) AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(Tuple(s IPv6), UInt8), `t.keys.s` Array(String)');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(Tuple(s String), UInt8), `t.keys.s` Array(IPv6)');
SELECT CAST(map(1, tuple('abcdefghijklmnop')) AS Map(UInt8, Tuple(s String))) AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(UInt8, Tuple(s IPv6)), `t.values.s` Array(String)');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(UInt8, Tuple(s String)), `t.values.s` Array(IPv6)');

SELECT 'null struct row';
SELECT CAST(if(number = 1, NULL, tuple('abcdefghijklmnop')) AS Nullable(Tuple(s String))) AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream SETTINGS enable_nullable_tuple_type = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't String, `t.s` IPv6') SETTINGS enable_nullable_tuple_type = 0;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(s IPv6), `t.s` String') SETTINGS enable_nullable_tuple_type = 0;

SELECT 'null outer struct row';
SELECT CAST(if(number = 1, NULL, tuple(tuple('abcdefghijklmnop'))) AS Nullable(Tuple(u Tuple(s String)))) AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream SETTINGS enable_nullable_tuple_type = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.u` String, `t.u.s` IPv6') SETTINGS enable_nullable_tuple_type = 0;
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.u` Tuple(s IPv6), `t.u.s` String') SETTINGS enable_nullable_tuple_type = 0;

SELECT 'null struct row with string subcolumns only';
SELECT CAST(if(number = 1, NULL, tuple('abcdefghijklmnop')) AS Nullable(Tuple(s String))) AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream SETTINGS enable_nullable_tuple_type = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't String, `t.s` String') SETTINGS enable_nullable_tuple_type = 0, input_format_null_as_default = 0;
SELECT t FROM file('data.arrows', 'ArrowStream', 't String, `t.s` IPv6') SETTINGS enable_nullable_tuple_type = 0, input_format_null_as_default = 0;

SELECT 'dictionary of structs';
EOF

# Written by PyArrow: a dictionary-encoded `struct<s: binary not null>` field whose dictionary holds `abcdefghijklmnop`.
echo "/////8gAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAEAAAAuP///wQAAAABAAAAFAAAABAAGAAIAAYABwAMABAAFAAQAAAAAAABDRgAAABAAAAAIAAAAAQAAAABAAAARAAAAAEAAAB0AAAACAAIAAAABAAIAAAADAAAAAgADAAIAAcACAAAAAAAAAEgAAAAzP///xAAFAAIAAAABwAMAAAAEAAQAAAAAAAABBAAAAAYAAAABAAAAAAAAAABAAAAcwAAAAQABAAEAAAAAAAAAP/////IAAAAFAAAAAAAAAAMABQABgAFAAgADAAMAAAAAAIEABQAAAAYAAAAAAAAAAgACgAAAAQACAAAABAAAAAAAAoAGAAMAAQACAAKAAAAXAAAABAAAAABAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAAAAAAAAQAAAAAAAAAAAAAAACAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAGFiY2RlZmdoaWprbG1ub3D/////iAAAABQAAAAAAAAADAAWAAYABQAIAAwADAAAAAADBAAYAAAACAAAAAAAAAAAAAoAGAAMAAQACAAKAAAAPAAAABAAAAABAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP////8AAAAA" | base64 -d > data.arrows

$CLICKHOUSE_LOCAL <<'EOF'
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(s IPv6), `t.s` String');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(s String), `t.s` IPv6');

SELECT 'dictionary of arrays of structs';
EOF

# Written by PyArrow: a dictionary-encoded `list<struct<s: binary not null>>` field.
echo "/////wABAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAEAAAAuP///wQAAAABAAAAFAAAABAAGAAIAAYABwAMABAAFAAQAAAAAAABDBgAAABAAAAAIAAAAAQAAAABAAAARAAAAAEAAAB0AAAACAAIAAAABAAIAAAADAAAAAgADAAIAAcACAAAAAAAAAEgAAAAkP///xAAFAAIAAYABwAMAAAAEAAQAAAAAAABDRQAAAAcAAAABAAAAAEAAAAkAAAABAAAAGl0ZW0AAAAAzP///xAAFAAIAAAABwAMAAAAEAAQAAAAAAAABBAAAAAYAAAABAAAAAAAAAABAAAAcwAAAAQABAAEAAAA//////gAAAAUAAAAAAAAAAwAFAAGAAUACAAMAAwAAAAAAgQAFAAAACAAAAAAAAAACAAKAAAABAAIAAAAEAAAAAAACgAYAAwABAAIAAoAAAB8AAAAEAAAAAEAAAAAAAAAAAAAAAYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAwAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAABAAAABhYmNkZWZnaGlqa2xtbm9w/////4gAAAAUAAAAAAAAAAwAFgAGAAUACAAMAAwAAAAAAwQAGAAAAAgAAAAAAAAAAAAKABgADAAEAAgACgAAADwAAAAQAAAAAQAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAEAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/////AAAAAA==" | base64 -d > data.arrows

$CLICKHOUSE_LOCAL <<'EOF'
SELECT * FROM file('data.arrows', 'ArrowStream', 't Array(Tuple(s IPv6)), `t.s` Array(String)');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Array(Tuple(s String)), `t.s` Array(IPv6)');

SELECT 'map with custom entry names';
EOF

# Written by PyArrow: `map<k: binary, v: binary>` holding `abcdefghijklmnop` as both the key and the value.
echo "//////gAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAEAAAAEAAAAqP///wAAAREUAAAAGAAAAAQAAAABAAAAEAAAAAEAAAB0AAAAaP///5z///8AAAANGAAAACAAAAAEAAAAAgAAAFwAAAAkAAAABwAAAGVudHJpZXMAmP///xAAFAAIAAYABwAMAAAAEAAQAAAAAAABBBAAAAAUAAAABAAAAAAAAAABAAAAdgAAAMz///8QABQACAAAAAcADAAAABAAEAAAAAAAAAQQAAAAGAAAAAQAAAAAAAAAAQAAAGsAAAAEAAQABAAAAP////8oAQAAFAAAAAAAAAAMABYABgAFAAgADAAMAAAAAAMEABgAAAA4AAAAAAAAAAAACgAYAAwABAAIAAoAAACsAAAAEAAAAAEAAAAAAAAAAAAAAAkAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAIAAAAAAAAACgAAAAAAAAAEAAAAAAAAAAAAAAABAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAQAAAAYWJjZGVmZ2hpamtsbW5vcAAAAAAQAAAAYWJjZGVmZ2hpamtsbW5vcP////8AAAAA" | base64 -d > data.arrows

$CLICKHOUSE_LOCAL <<'EOF'
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(IPv6, String), `t.keys` Array(String)');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(String, IPv6), `t.values` Array(String)');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.keys` Array(IPv6), `t.values` Array(UInt128)');

SELECT 'dictionary of structs with a UUID leaf';
EOF

# Written by PyArrow: a dictionary-encoded `struct<s: fixed_size_binary(16) not null>` field whose dictionary holds `abcdefghijklmnop`.
echo "/////9AAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAEAAAAuP///wQAAAABAAAAFAAAABAAGAAIAAYABwAMABAAFAAQAAAAAAABDRgAAABEAAAAIAAAAAQAAAABAAAASAAAAAEAAAB0AAAACAAIAAAABAAIAAAADAAAAAgADAAIAAcACAAAAAAAAAEgAAAABAAEAAQAAAAQABQACAAAAAcADAAAABAAEAAAAAAAAA8QAAAAGAAAAAQAAAAAAAAAAQAAAHMABgAIAAQABgAAABAAAAAAAAAA/////7gAAAAUAAAAAAAAAAwAFAAGAAUACAAMAAwAAAAAAgQAFAAAABAAAAAAAAAACAAKAAAABAAIAAAAEAAAAAAACgAYAAwABAAIAAoAAABMAAAAEAAAAAEAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAgAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAYWJjZGVmZ2hpamtsbW5vcP////+IAAAAFAAAAAAAAAAMABYABgAFAAgADAAMAAAAAAMEABgAAAAIAAAAAAAAAAAACgAYAAwABAAIAAoAAAA8AAAAEAAAAAEAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAABAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/////wAAAAA=" | base64 -d > data.arrows

$CLICKHOUSE_LOCAL <<'EOF'
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(s UUID), `t.s` String');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(s String), `t.s` UUID');

SELECT 'map key lookups';
SELECT map(1, 'abcdefghijklmnop') AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(UInt8, IPv6), `t.key_1` String');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.values` Array(IPv6), `t.key_1` String');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(UInt8, String), `t.key_1` IPv6');

SELECT 'case-insensitive names';
SELECT CAST(tuple('abcdefghijklmnop') AS Tuple(s String)) AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 'T String, t Tuple(s IPv6)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(s IPv6), T String') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
SELECT 'abcdefghijklmnop' AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 'T String, t IPv6') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't IPv6, T String') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;

SELECT 'case-insensitive names with a numeric date';
EOF

# Written by PyArrow: a `date32` field holding the day number 3000000, beyond the `Date32` range.
echo "/////3AAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAEAAAAUAAAAEAAUAAgABgAHAAwAAAAQABAAAAAAAAEIEAAAABgAAAAEAAAAAAAAAAEAAAB0AAYACAAGAAYAAAAAAAAA/////4gAAAAUAAAAAAAAAAwAFgAGAAUACAAMAAwAAAAAAwQAGAAAAAgAAAAAAAAAAAAKABgADAAEAAgACgAAADwAAAAQAAAAAQAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAEAAAABAAAAAAAAAAAAAAAAAAAAwMYtAAAAAAD/////AAAAAA==" | base64 -d > data.arrows

$CLICKHOUSE_LOCAL <<'EOF'
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Int32, t Int64') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Int64, T Int32') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;

SELECT 'case-insensitive names with a null struct row';
SELECT CAST(if(number = 1, NULL, tuple('abcdefghijklmnop')) AS Nullable(Tuple(s String))) AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream SETTINGS enable_nullable_tuple_type = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Tuple(s IPv6), t String') SETTINGS input_format_arrow_case_insensitive_column_matching = 1, enable_nullable_tuple_type = 0;
SELECT * FROM file('data.arrows', 'ArrowStream', 't String, T Tuple(s IPv6)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1, enable_nullable_tuple_type = 0;

SELECT 'case-insensitive names with a top-level dictionary';
EOF

# Written by PyArrow: a dictionary-encoded `binary` field whose dictionary holds only `abcdefghijklmnop`.
echo "/////5AAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAEAAAAvP///wQAAAABAAAAFAAAABAAGAAIAAYABwAMABAAFAAQAAAAAAABBBQAAABAAAAAHAAAAAQAAAAAAAAAAQAAAHQAAAAIAAgAAAAEAAgAAAAMAAAACAAMAAgABwAIAAAAAAAAASAAAAAEAAQABAAAAAAAAAD/////qAAAABQAAAAAAAAADAAUAAYABQAIAAwADAAAAAACBAAUAAAAGAAAAAAAAAAIAAoAAAAEAAgAAAAQAAAAAAAKABgADAAEAAgACgAAAEwAAAAQAAAAAQAAAAAAAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAAAAAAAAQAAAAAAAAAAAAAAABAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAYWJjZGVmZ2hpamtsbW5vcP////+IAAAAFAAAAAAAAAAMABYABgAFAAgADAAMAAAAAAMEABgAAAAIAAAAAAAAAAAACgAYAAwABAAIAAoAAAA8AAAAEAAAAAIAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAABAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/////wAAAAA=" | base64 -d > data.arrows

$CLICKHOUSE_LOCAL <<'EOF'
SELECT * FROM file('data.arrows', 'ArrowStream', 'T IPv6, t String') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't String, T IPv6') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;

SELECT 'null struct row with a valid nullable child';
EOF

# Written by PyArrow: a nullable `struct<s: binary>` whose second row is null while the child slot below it is valid.
echo "/////5gAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAEAAAAEAAAA2P///wAAAQ0UAAAAGAAAAAQAAAABAAAAIAAAAAEAAAB0AAAAzP///xAAFAAIAAYABwAMAAAAEAAQAAAAAAABBBAAAAAYAAAABAAAAAAAAAABAAAAcwAAAAQABAAEAAAAAAAAAP////+4AAAAFAAAAAAAAAAMABYABgAFAAgADAAMAAAAAAMEABgAAAA4AAAAAAAAAAAACgAYAAwABAAIAAoAAABcAAAAEAAAAAIAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAEAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAADAAAAAAAAAAYAAAAAAAAACAAAAAAAAAAAAAAAAIAAAACAAAAAAAAAAEAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAABAAAAAgAAAAAAAAAGFiY2RlZmdoaWprbG1ub3BhYmNkZWZnaGlqa2xtbm9w/////wAAAAA=" | base64 -d > data.arrows

$CLICKHOUSE_LOCAL <<'EOF'
SELECT * FROM file('data.arrows', 'ArrowStream', 't String') SETTINGS enable_nullable_tuple_type = 0;
SELECT * FROM file('data.arrows', 'ArrowStream', 't String, `t.s` IPv6') SETTINGS enable_nullable_tuple_type = 0;


SELECT 'null inner struct row requested as a column and inside its parent';
SELECT CAST(tuple(if(number = 1, NULL, tuple('abcdefghijklmnop'))) AS Tuple(u Nullable(Tuple(s String)))) AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream SETTINGS enable_nullable_tuple_type = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(u String), `t.u` Tuple(s IPv6)') SETTINGS enable_nullable_tuple_type = 0;
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.u` Tuple(s IPv6), t Tuple(u String)') SETTINGS enable_nullable_tuple_type = 0;
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Tuple(u String), t Tuple(u Tuple(s IPv6))') SETTINGS input_format_arrow_case_insensitive_column_matching = 1, enable_nullable_tuple_type = 0;
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Tuple(u Tuple(s IPv6)), t Tuple(u String)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1, enable_nullable_tuple_type = 0;

SELECT 'null struct row of map values';
SELECT CAST(map(1, if(number = 1, NULL, tuple('abcdefghijklmnop'))) AS Map(UInt8, Nullable(Tuple(s String)))) AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream SETTINGS enable_nullable_tuple_type = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(UInt8, String), `t.values` Array(Tuple(s IPv6))') SETTINGS enable_nullable_tuple_type = 0;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(UInt8, String), `t.key_1` Tuple(s IPv6)') SETTINGS enable_nullable_tuple_type = 0;

SELECT 'map row without the requested key';
SELECT map(toUInt8(number + 1), 'abcdefghijklmnop') AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(UInt8, IPv6), `t.key_1` IPv6');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.key_1` IPv6, t Map(UInt8, IPv6)');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(UInt8, String), `t.key_1` IPv6');

SELECT 'case-insensitive names with a dictionary of dates read as decimals';
SELECT toLowCardinality(toDate32('2024-01-02')) AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream SETTINGS output_format_arrow_low_cardinality_as_dictionary = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Decimal64(0), t Decimal32(0)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;

SELECT 'case-insensitive names with a date inside structs';
SELECT CAST(tuple(d) AS Tuple(d Date32)) AS t FROM format(RowBinary, 'd Date32', unhex('c0c62d00')) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Tuple(d String), t Tuple(d Int32)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Tuple(d Int32), t Tuple(d String)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;

SELECT 'array of maps without the requested key';
SELECT [map(toUInt8(number + 1), 'abcdefghijklmnop')] AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Array(Map(UInt8, IPv6)), `t.key_1` Array(IPv6)');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.key_1` Array(IPv6), t Array(Map(UInt8, IPv6))');

SELECT 'map lookup of a raw-byte key';
SELECT map('abcdefghijklmnop', toUInt8(7)) AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(IPv6, UInt8), `t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8, t Map(IPv6, UInt8)');

SELECT 'case-insensitive names of a subcolumn of a null struct row';
SELECT CAST(if(number = 1, NULL, tuple('abcdefghijklmnop')) AS Nullable(Tuple(s String))) AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream SETTINGS enable_nullable_tuple_type = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.s` String, `T.S` IPv6') SETTINGS input_format_arrow_case_insensitive_column_matching = 1, enable_nullable_tuple_type = 0;
SELECT * FROM file('data.arrows', 'ArrowStream', '`T.S` IPv6, `t.s` String') SETTINGS input_format_arrow_case_insensitive_column_matching = 1, enable_nullable_tuple_type = 0;

SELECT 'case-insensitive names with a date inside named and unnamed structs';
SELECT CAST(tuple(d) AS Tuple(d Date32)) AS t FROM format(RowBinary, 'd Date32', unhex('c0c62d00')) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Tuple(String), t Tuple(d Int32)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(d Int32), T Tuple(String)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Tuple(Int32), t Tuple(d String)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;

SELECT 'map of maps without the requested key';
SELECT map(toUInt8(1), map(toUInt8(number + 1), 'abcdefghijklmnop')) AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(UInt8, Map(UInt8, IPv6)), `t.values.key_1` Array(IPv6)');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.values.key_1` Array(IPv6), t Map(UInt8, Map(UInt8, IPv6))');

SELECT 'map lookup of a dotted key';
SELECT map(if(number = 0, 'a.b', 'c.d'), 'abcdefghijklmnop') AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(String, IPv6), `t.key_a.b` IPv6');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.key_a.b` IPv6, t Map(String, IPv6)');

SELECT 'map lookup of a raw-byte key requested through the keys';
SELECT map('abcdefghijklmnop', toUInt8(7)) AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.keys` Array(IPv6), `t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8, `t.keys` Array(IPv6)');

SELECT 'null struct row of map values with a key lookup';
SELECT CAST(map(1, if(number = 1, NULL, tuple('abcdefghijklmnop'))) AS Map(UInt8, Nullable(Tuple(s String)))) AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream SETTINGS enable_nullable_tuple_type = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.values.s` Array(IPv6), `t.key_1` Tuple(s String)') SETTINGS enable_nullable_tuple_type = 0;
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.key_1` Tuple(s String), `t.values.s` Array(IPv6)') SETTINGS enable_nullable_tuple_type = 0;

SELECT 'map without the requested key below a dotted struct field';
SELECT CAST(tuple(map(toUInt8(number + 1), 'abcdefghijklmnop')) AS Tuple(`u.m` Map(UInt8, String))) AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(`u.m` Map(UInt8, IPv6)), `t.u.m.key_1` IPv6');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.u.m.key_1` IPv6, t Tuple(`u.m` Map(UInt8, IPv6))');

SELECT 'map without the requested key below an unnamed struct';
SELECT CAST(tuple(map(toUInt8(number + 1), 'abcdefghijklmnop')) AS Tuple(Map(UInt8, String))) AS t FROM numbers(2) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(Map(UInt8, IPv6)), `t.1.key_1` IPv6');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.1.key_1` IPv6, t Tuple(Map(UInt8, IPv6))');

SELECT 'map lookup of a raw-byte key requested through a subcolumn of the keys';
SELECT CAST(map(tuple('abcdefghijklmnop'), toUInt8(7)) AS Map(Tuple(s String), UInt8)) AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.keys.s` Array(IPv6), `t.key_(''6162:6364:6566:6768:696a:6b6c:6d6e:6f70'')` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.key_(''6162:6364:6566:6768:696a:6b6c:6d6e:6f70'')` UInt8, `t.keys.s` Array(IPv6)');

SELECT 'case-insensitive names of structs with different field names';
SELECT CAST(tuple(toDate32('2024-01-01')) AS Tuple(x Date32)) AS t, CAST(tuple(d) AS Tuple(d Date32)) AS T FROM format(RowBinary, 'd Date32', unhex('c0c62d00')) INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Tuple(d String), t Tuple(Int32)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(Int32), T Tuple(d String)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
EOF

cd "$CLICKHOUSE_TMP" || exit 1
rm -rf "$dir"
