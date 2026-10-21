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

SELECT a, b FROM t ORDER BY a;

DROP TABLE t;
