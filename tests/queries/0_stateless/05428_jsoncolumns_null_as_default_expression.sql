-- https://github.com/ClickHouse/ClickHouse/issues/121905
-- An explicit null in JSONColumns-like formats should take the column's DEFAULT expression
-- with input_format_null_as_default, the same as in JSONEachRow.

DROP TABLE IF EXISTS t;
CREATE TABLE t (a UInt32, b UInt32 DEFAULT a*2) ENGINE = MergeTree ORDER BY a;

INSERT INTO t SETTINGS input_format_null_as_default = 1 FORMAT JSONEachRow {"a":5,"b":null}

INSERT INTO t SETTINGS input_format_null_as_default = 1 FORMAT JSONColumns {"a":[6],"b":[null]}

INSERT INTO t SETTINGS input_format_null_as_default = 1 FORMAT JSONCompactColumns [[7],[null]]

INSERT INTO t SETTINGS input_format_null_as_default = 1 FORMAT JSONColumnsWithMetadata {"meta":[{"name":"a","type":"UInt32"},{"name":"b","type":"UInt32"}],"data":{"a":[8],"b":[null]}}

-- Omitting the "b" column entirely gives a*2 as well.
INSERT INTO t SETTINGS input_format_null_as_default = 1 FORMAT JSONColumns {"a":[9]}

-- Only the null rows take the DEFAULT, 100 stays.
INSERT INTO t SETTINGS input_format_null_as_default = 1 FORMAT JSONColumns {"a":[10,11,12],"b":[null,100,null]}

-- With input_format_defaults_for_omitted_fields = 0 a null stays 0, as in JSONEachRow.
INSERT INTO t SETTINGS input_format_null_as_default = 1, input_format_defaults_for_omitted_fields = 0 FORMAT JSONColumns {"a":[13],"b":[null]}

-- An empty string with input_format_json_empty_as_default = 1 takes the DEFAULT too.
INSERT INTO t SETTINGS input_format_json_empty_as_default = 1 FORMAT JSONColumns {"a":[14],"b":[""]}

SELECT a, b FROM t ORDER BY a;

DROP TABLE t;

-- Reading only "b" also reads "a" for its DEFAULT.
INSERT INTO FUNCTION file(currentDatabase() || '_05428.json', JSONColumns, 'a UInt32, b Nullable(UInt32)')
    SELECT * FROM values('a UInt32, b Nullable(UInt32)', (6, NULL), (7, 3)) SETTINGS engine_file_truncate_on_insert = 1;

SELECT b FROM file(currentDatabase() || '_05428.json', JSONColumns, 'a UInt32, b UInt32 DEFAULT a*2');
SELECT b FROM file(currentDatabase() || '_05428.json', JSONColumns, 'a UInt32, b UInt32 DEFAULT a*2, c UInt32 DEFAULT b + 1');
SELECT c FROM file(currentDatabase() || '_05428.json', JSONColumns, 'a UInt32, b UInt32 DEFAULT a*2, c UInt32 DEFAULT b + 1');

-- A DEFAULT which depends on a subcolumn reads its column.
INSERT INTO FUNCTION file(currentDatabase() || '_05428_tuple.json', JSONColumns, 'a Tuple(x UInt32), b Nullable(UInt32)')
    SELECT * FROM values('a Tuple(x UInt32), b Nullable(UInt32)', (tuple(6), NULL), (tuple(7), 3)) SETTINGS engine_file_truncate_on_insert = 1;

SELECT b FROM file(currentDatabase() || '_05428_tuple.json', JSONColumns, 'a Tuple(x UInt32), b UInt32 DEFAULT a.x * 2');

-- Without input_format_defaults_for_omitted_fields the columns of DEFAULT expressions are not read (here "a" cannot be parsed).
INSERT INTO FUNCTION file(currentDatabase() || '_05428_unparsable.json', JSONColumns, 'a String, b UInt32')
    SELECT * FROM values('a String, b UInt32', ('oops', 3)) SETTINGS engine_file_truncate_on_insert = 1;

SELECT b FROM file(currentDatabase() || '_05428_unparsable.json', JSONColumns, 'a UInt32, b UInt32 DEFAULT a * 2')
    SETTINGS input_format_defaults_for_omitted_fields = 0;

-- The default of a column which is read only for a DEFAULT is computed only if that DEFAULT is computed:
-- "b" is present, so the default of "c" (which fails for "oops") is not needed.
SELECT b FROM file(currentDatabase() || '_05428_unparsable.json', JSONColumns, 'a String, c UInt32 DEFAULT toUInt32(a), b UInt32 DEFAULT c * 2');
-- "d" is not in the file, its default is computed for the missing "b".
SELECT b FROM file(currentDatabase() || '_05428.json', JSONColumns, 'a UInt32, d UInt32 DEFAULT a + 1, b UInt32 DEFAULT d * 2');
