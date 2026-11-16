SET engine_file_truncate_on_insert = 1;

-- A stored column `v.tag` next to `v`, used only in PREWHERE.
INSERT INTO FUNCTION file(currentDatabase() || '_dotted.parquet', Parquet, 'v UInt64, `v.tag` UInt64') SELECT number, number % 2 FROM numbers(4);
SELECT v FROM file(currentDatabase() || '_dotted.parquet', Parquet, 'v UInt64, `v.tag` UInt64') PREWHERE `v.tag` = 1 ORDER BY v;

-- File engine; a dotted column next to a Tuple, and one named like a subcolumn of a Nullable.
CREATE TABLE t_dotted (t Tuple(a UInt64), `t.b` UInt64, n Nullable(UInt64), `n.null` UInt8) ENGINE = File(Parquet);
INSERT INTO t_dotted SELECT tuple(number), number % 2, number, number % 2 FROM numbers(4);
SELECT t FROM t_dotted PREWHERE `t.b` = 1 ORDER BY t;
SELECT n FROM t_dotted PREWHERE `n.null` = 1 ORDER BY n;
DROP TABLE t_dotted;
