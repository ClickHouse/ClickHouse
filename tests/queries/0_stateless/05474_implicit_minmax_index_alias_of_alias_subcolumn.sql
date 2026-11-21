-- An ALIAS over a subcolumn of another ALIAS cannot be expanded to physical columns for an index.
-- The implicit min-max index is not created for it, and the table is created and reloaded as usual.

DROP TABLE IF EXISTS t;

CREATE TABLE t
(
    a UInt64,
    b Tuple(v UInt64) ALIAS tuple(a),
    c UInt64 ALIAS b.v + 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t SELECT number FROM numbers(10);
SELECT count() FROM t WHERE c = 5;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't' ORDER BY name;

DETACH TABLE t;
ATTACH TABLE t;
SELECT count() FROM t WHERE c = 5;

DROP TABLE t;
