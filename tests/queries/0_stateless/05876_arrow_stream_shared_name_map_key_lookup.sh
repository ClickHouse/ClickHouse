#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `ArrowStream` format needs Arrow, which is not built in the fast test.

# With case-insensitive matching, columns that share a name can request different representations of the keys
# of a map (`T Map(String, UInt8)` and `t Map(IPv6, UInt8)`). A key lookup (`t.key_...`) parses its key in the
# type of the keys, so it must be taken from the representation that resolves it, whatever the order of the
# columns in the header.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$dir"
mkdir -p "$dir"
cd "$dir" || exit 1

$CLICKHOUSE_LOCAL <<'EOF'
SET input_format_arrow_case_insensitive_column_matching = 1;

SELECT map('abcdefghijklmnop', toUInt8(7)) AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;

SELECT 'key in the converted representation';
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Map(String, UInt8), t Map(IPv6, UInt8), `t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(IPv6, UInt8), T Map(String, UInt8), `t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8, T Map(String, UInt8), t Map(IPv6, UInt8)');

SELECT 'key in the decoded representation';
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Map(String, UInt8), t Map(IPv6, UInt8), `t.key_abcdefghijklmnop` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(IPv6, UInt8), T Map(String, UInt8), `t.key_abcdefghijklmnop` UInt8');

SELECT 'keys in both representations';
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Map(String, UInt8), t Map(IPv6, UInt8), `t.key_abcdefghijklmnop` UInt8, `t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(IPv6, UInt8), T Map(String, UInt8), `t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8, `t.key_abcdefghijklmnop` UInt8');

SELECT 'missing key';
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Map(String, UInt8), t Map(IPv6, UInt8), `t.key_missing` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(IPv6, UInt8), T Map(String, UInt8), `t.key_missing` UInt8');

SELECT 'requested through the keys';
SELECT * FROM file('data.arrows', 'ArrowStream', '`T.keys` Array(String), `t.keys` Array(IPv6), `t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.keys` Array(IPv6), `T.keys` Array(String), `t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', '`T.keys` Array(String), `t.keys` Array(IPv6), `t.key_abcdefghijklmnop` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', '`t.keys` Array(IPv6), `T.keys` Array(String), `t.key_abcdefghijklmnop` UInt8');

SELECT 'a whole column that does not describe the keys';
SELECT * FROM file('data.arrows', 'ArrowStream', 'T String, t Map(IPv6, UInt8), `t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Map(IPv6, UInt8), T String, `t.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');

SELECT 'map below a struct';
SELECT CAST(tuple(map('abcdefghijklmnop', toUInt8(7))) AS Tuple(m Map(String, UInt8))) AS t INTO OUTFILE 'data.arrows' TRUNCATE FORMAT ArrowStream;
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Tuple(m Map(String, UInt8)), t Tuple(m Map(IPv6, UInt8)), `t.m.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(m Map(IPv6, UInt8)), T Tuple(m Map(String, UInt8)), `t.m.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', 'T Tuple(m Map(String, UInt8)), t Tuple(m Map(IPv6, UInt8)), `t.m.key_abcdefghijklmnop` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(m Map(IPv6, UInt8)), T Tuple(m Map(String, UInt8)), `t.m.key_abcdefghijklmnop` UInt8');

SELECT 'shared names of both the struct and the map';
SELECT * FROM file('data.arrows', 'ArrowStream', 'T String, t Tuple(m Map(String, UInt8)), `T.M` Map(String, UInt8), `t.m` Map(IPv6, UInt8), `t.m.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');
SELECT * FROM file('data.arrows', 'ArrowStream', 't Tuple(m Map(String, UInt8)), T String, `t.m` Map(IPv6, UInt8), `T.M` Map(String, UInt8), `t.m.key_6162:6364:6566:6768:696a:6b6c:6d6e:6f70` UInt8');
EOF

cd "$CLICKHOUSE_TMP" || exit 1
rm -rf "$dir"
