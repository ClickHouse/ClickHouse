-- Tags: shard

-- With `prefer_column_name_to_alias = 1` an INTERPOLATE target named like a selected alias can resolve to a source column;
-- the alias column then keeps its own values.
SET prefer_column_name_to_alias = 1;

SELECT 2 * number AS k, x + 1 AS x FROM (SELECT number, number AS x FROM numbers(2))
ORDER BY k WITH FILL INTERPOLATE (x AS x + 10);

SELECT 2 * number AS k, x * 3 AS x FROM (SELECT number, number AS x FROM numbers(2))
ORDER BY k WITH FILL INTERPOLATE (x);

SELECT k, x FROM (SELECT 2 * number AS k, x + 1 AS x FROM (SELECT number, number AS x FROM numbers(2))
ORDER BY k WITH FILL INTERPOLATE (x AS x + 10));

DROP TABLE IF EXISTS t_interpolate_prefer;
CREATE TABLE t_interpolate_prefer (number UInt64, x UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO t_interpolate_prefer VALUES (0, 0), (1, 1);
SELECT 2 * number AS k, x + 1 AS x FROM remote('127.0.0.2', currentDatabase(), t_interpolate_prefer)
ORDER BY k WITH FILL INTERPOLATE (x AS x + 10);
DROP TABLE t_interpolate_prefer;

SELECT 2 * number AS k, x + 1 AS x FROM (SELECT number, number AS x FROM numbers(2))
ORDER BY k WITH FILL INTERPOLATE (x AS x + 10) SETTINGS prefer_column_name_to_alias = 0;
