-- A column TTL on one `Nested` subcolumn resets it to arrays of defaults of the same size,
-- because its array sizes are shared with the other subcolumns, which keep their values.

DROP TABLE IF EXISTS t_nested_ttl_wide;
DROP TABLE IF EXISTS t_nested_ttl_rows;
DROP TABLE IF EXISTS t_nested_ttl_compact;

-- A Wide part with every row expired, the type change also rewrites the subcolumn.
CREATE TABLE t_nested_ttl_wide (id UInt64, ts DateTime, n Nested(a String, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_nested_ttl_wide VALUES (1, '2020-01-01 00:00:00', ['a', 'b'], ['x', 'y']);
ALTER TABLE t_nested_ttl_wide MODIFY COLUMN `n.a` Array(Nullable(String)) TTL ts + INTERVAL 1 DAY, MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'wide', id, `n.a`, `n.b` FROM t_nested_ttl_wide;

-- Only some rows of the part are expired.
CREATE TABLE t_nested_ttl_rows (id UInt64, ts DateTime, n Nested(a String, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_nested_ttl_rows VALUES (1, '2020-01-01 00:00:00', ['a', 'b'], ['x', 'y']), (2, '2100-01-01 00:00:00', ['c'], ['z']);
ALTER TABLE t_nested_ttl_rows MODIFY COLUMN `n.a` Array(String) TTL ts + INTERVAL 1 DAY SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE t_nested_ttl_rows FINAL;
SELECT 'rows', id, `n.a`, `n.b` FROM t_nested_ttl_rows ORDER BY id;

-- A Compact part stores the sizes of each subcolumn separately, but it gets the same default.
CREATE TABLE t_nested_ttl_compact (id UInt64, ts DateTime, n Nested(a String, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000;

INSERT INTO t_nested_ttl_compact VALUES (1, '2020-01-01 00:00:00', ['a', 'b'], ['x', 'y']);
ALTER TABLE t_nested_ttl_compact MODIFY COLUMN `n.a` Array(String) TTL ts + INTERVAL 1 DAY, MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'compact', id, `n.a`, `n.b` FROM t_nested_ttl_compact;

DROP TABLE t_nested_ttl_wide;
DROP TABLE t_nested_ttl_rows;
DROP TABLE t_nested_ttl_compact;
