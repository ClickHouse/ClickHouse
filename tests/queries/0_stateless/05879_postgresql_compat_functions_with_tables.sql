-- The PostgreSQL compatibility functions over the columns of a table: `String`, `Nullable(String)` and
-- `LowCardinality(String)` columns, a position and a dimension taken from a column, and the functions in
-- `WHERE`, `GROUP BY` and `arrayJoin`.

DROP TABLE IF EXISTS t_postgresql_compat;
CREATE TABLE t_postgresql_compat
(
    id UInt32,
    s String,
    ns Nullable(String),
    lc LowCardinality(String),
    n Int32,
    a Array(UInt32),
    nested Array(Array(UInt8)),
    d UInt8,
    ts Int64,
    f Float64
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_postgresql_compat VALUES
    (1, 'a,b,c', 'x,y', 'p,q', 2, [1, 2, 3], [[1], [2, 3]], 1, 1700000000, 1.5),
    (2, '', NULL, 'p,q', 1, [], [], 1, 0, 0.25),
    (3, 'no delimiter', 'z,', 'r', -1, [7], [[]], 2, -1, 1700000000.125),
    (4, ',lead', ',', 'p,q', 3, [4, 5], [[1, 2], [3]], 1, 86400, 86400);

SELECT '-- split_part';
SELECT id, [split_part(s, ',', 2), split_part(lc, ',', 2), split_part(s, ',', n), split_part(s, ',', -1)], split_part(ns, ',', 1) FROM t_postgresql_compat ORDER BY id;
SELECT id FROM t_postgresql_compat WHERE split_part(s, ',', 1) = 'a' ORDER BY id;
SELECT split_part(lc, ',', 1) AS k, count() FROM t_postgresql_compat GROUP BY k ORDER BY k;

SELECT '-- string_to_array';
SELECT id, string_to_array(s, ','), string_to_array(ns, ','), string_to_array(lc, ','), string_to_array(s, ',', 'b') FROM t_postgresql_compat ORDER BY id;
SELECT id, arrayJoin(string_to_array(s, ',')) AS part FROM t_postgresql_compat ORDER BY id, part;
SELECT id, length(string_to_array(s, ' ')) FROM t_postgresql_compat ORDER BY id;

SELECT '-- array_length';
SELECT id, array_length(a, 1), array_length(nested, 1), array_length(a, d), array_length(a, 0) FROM t_postgresql_compat ORDER BY id;
SELECT id FROM t_postgresql_compat WHERE array_length(a, 1) IS NULL ORDER BY id;
SELECT sum(array_length(a, 1)) FROM t_postgresql_compat;

SELECT '-- regexp_split_to_array and regexp_split_to_table';
SELECT id, regexp_split_to_array(s, '[, ]') FROM t_postgresql_compat ORDER BY id;
SELECT id, regexp_split_to_table(s, ',') AS part FROM t_postgresql_compat ORDER BY id, part;
SELECT count() FROM (SELECT regexp_split_to_table(s, ',') FROM t_postgresql_compat);

SELECT '-- to_timestamp';
SELECT id, to_timestamp(ts), to_timestamp(f) FROM t_postgresql_compat ORDER BY id SETTINGS session_timezone = 'UTC';
SELECT id FROM t_postgresql_compat WHERE to_timestamp(ts) >= toDateTime64('2023-01-01 00:00:00', 6, 'UTC') ORDER BY id;

SELECT '-- random';
SELECT count(), countIf(r >= 0 AND r < 1) FROM (SELECT random() AS r FROM t_postgresql_compat);

DROP TABLE t_postgresql_compat;
