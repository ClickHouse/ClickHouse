#!/usr/bin/env bash
# With allow_nullable_tuple_in_extracted_subcolumns = 0 a Tuple subcolumn of a Variant, a Dynamic or a JSON path is not
# Nullable, so a row that does not hold the Tuple reads its type default ('', 'a'). The setting is read from the global
# context, so it is passed on the command line.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

T="Tuple(s String, e Enum8('a' = 1, 'b' = 2))"
# The same type inside a string literal.
L="Tuple(s String, e Enum8(''a'' = 1, ''b'' = 2))"

# The second insert holds no Tuple at all. Merges are stopped so that each MergeTree table keeps one part of each kind,
# and the part thresholds pin the compact and the wide reader.
${CLICKHOUSE_LOCAL} --allow_nullable_tuple_in_extracted_subcolumns=0 --query "
CREATE TABLE v_memory (id UInt8, v Variant(String, $T)) ENGINE = Memory;
CREATE TABLE v_compact (id UInt8, v Variant(String, $T)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1e9, min_rows_for_wide_part = 1e9;
CREATE TABLE v_wide (id UInt8, v Variant(String, $T)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES v_compact;
SYSTEM STOP MERGES v_wide;
INSERT INTO v_memory VALUES (1, 'str'), (2, CAST(('x', 'b'), '$L')), (3, NULL);
INSERT INTO v_memory VALUES (4, 'only str'), (5, 'more str');
INSERT INTO v_compact SELECT * FROM v_memory WHERE id <= 3;
INSERT INTO v_compact SELECT * FROM v_memory WHERE id > 3;
INSERT INTO v_wide SELECT * FROM v_memory WHERE id <= 3;
INSERT INTO v_wide SELECT * FROM v_memory WHERE id > 3;

SELECT 'Variant', toTypeName(v.\`$T\`.e) FROM v_memory LIMIT 1;
SELECT 'Variant memory', id, v.\`$T\`, v.\`$T\`.e, variantElement(v, '$L') FROM v_memory ORDER BY id;
SELECT 'Variant compact', id, v.\`$T\`, v.\`$T\`.e FROM v_compact ORDER BY id;
SELECT 'Variant wide', id, v.\`$T\`, v.\`$T\`.e FROM v_wide ORDER BY id;

CREATE TABLE d_memory (id UInt8, d Dynamic) ENGINE = Memory;
CREATE TABLE d_compact (id UInt8, d Dynamic) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1e9, min_rows_for_wide_part = 1e9;
CREATE TABLE d_wide (id UInt8, d Dynamic) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES d_compact;
SYSTEM STOP MERGES d_wide;
INSERT INTO d_memory VALUES (1, 'str'), (2, CAST(('x', 'b'), '$L')), (3, NULL);
INSERT INTO d_memory VALUES (4, 'only str'), (5, 'more str');
INSERT INTO d_compact SELECT * FROM d_memory WHERE id <= 3;
INSERT INTO d_compact SELECT * FROM d_memory WHERE id > 3;
INSERT INTO d_wide SELECT * FROM d_memory WHERE id <= 3;
INSERT INTO d_wide SELECT * FROM d_memory WHERE id > 3;

SELECT 'Dynamic', toTypeName(d.\`$T\`.e) FROM d_memory LIMIT 1;
SELECT 'Dynamic memory', id, d.\`$T\`, d.\`$T\`.e, dynamicElement(d, '$L') FROM d_memory ORDER BY id;
SELECT 'Dynamic compact', id, d.\`$T\`, d.\`$T\`.e FROM d_compact ORDER BY id;
SELECT 'Dynamic wide', id, d.\`$T\`, d.\`$T\`.e FROM d_wide ORDER BY id;

-- Every type in the shared variant.
CREATE TABLE d0_memory (id UInt8, d Dynamic(max_types = 0)) ENGINE = Memory;
CREATE TABLE d0_wide (id UInt8, d Dynamic(max_types = 0)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO d0_memory VALUES (1, 'str'), (2, CAST(('x', 'b'), '$L')), (3, NULL);
INSERT INTO d0_wide SELECT * FROM d0_memory;
SELECT 'Dynamic(max_types = 0) memory', id, d.\`$T\`, d.\`$T\`.e FROM d0_memory ORDER BY id;
SELECT 'Dynamic(max_types = 0) wide', id, d.\`$T\`, d.\`$T\`.e FROM d0_wide ORDER BY id;

-- Path a in the shared data: a String in row 1 and absent in row 2, which is a granule of its own.
CREATE TABLE j_memory (id UInt8, j JSON(max_dynamic_paths = 0)) ENGINE = Memory;
CREATE TABLE j_advanced (id UInt8, j JSON(max_dynamic_paths = 0)) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    object_shared_data_serialization_version = 'advanced', object_shared_data_serialization_version_for_zero_level_parts = 'advanced';
CREATE TABLE j_map (id UInt8, j JSON(max_dynamic_paths = 0)) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    object_shared_data_serialization_version = 'map', object_shared_data_serialization_version_for_zero_level_parts = 'map';
INSERT INTO j_memory VALUES (1, '{\"a\": \"str\"}'), (2, '{\"b\": 1}');
INSERT INTO j_advanced SELECT * FROM j_memory;
INSERT INTO j_map SELECT * FROM j_memory;

SELECT 'JSON', toTypeName(j.a.:\`$T\`.e) FROM j_memory LIMIT 1;
SELECT 'JSON memory', id, j.a.:\`$T\`, j.a.:\`$T\`.e FROM j_memory ORDER BY id;
SELECT 'JSON advanced', id, j.a.:\`$T\`, j.a.:\`$T\`.e FROM j_advanced ORDER BY id;
SELECT 'JSON map', id, j.a.:\`$T\`, j.a.:\`$T\`.e FROM j_map ORDER BY id;
"

# With the default the subcolumn is Nullable and such a row is NULL.
${CLICKHOUSE_LOCAL} --query "
CREATE TABLE t (id UInt8, v Variant(String, $T), d Dynamic, j JSON(max_dynamic_paths = 0)) ENGINE = Memory;
INSERT INTO t VALUES (1, 'str', 'str', '{\"a\": \"str\"}');
SELECT 'default', toTypeName(v.\`$T\`), v.\`$T\`, d.\`$T\`, j.a.:\`$T\` FROM t;
"
