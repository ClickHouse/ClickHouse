-- https://github.com/ClickHouse/ClickHouse/issues/120936
-- GROUP BY on a JSON column with a nested array of objects must not split equal values
-- when only some blocks went through a filter.

DROP TABLE IF EXISTS t_json_group_by_path_order;
CREATE TABLE t_json_group_by_path_order (id UInt64, data JSON) ENGINE = Memory;

INSERT INTO t_json_group_by_path_order VALUES (0, '{"images":[{"url":"a","width":100}]}'), (1, '{"images":[{"url":"a","width":100}]}'), (2, '{"images":[{"url":"a","width":100}]}');
INSERT INTO t_json_group_by_path_order VALUES (3, '{"images":[{"url":"a","width":100}]}'), (4, '{"images":[{"url":"a","width":100}]}'), (5, '{"images":[{"url":"a","width":100}]}');

SELECT toString(data), count() FROM t_json_group_by_path_order WHERE id != 0 GROUP BY data SETTINGS max_threads = 1;
SELECT uniqExact(data), count(DISTINCT data), count() FROM t_json_group_by_path_order WHERE id != 0 SETTINGS max_threads = 1;

-- RowBinary representation of equal values must not depend on the column layout either.
SELECT uniqExact(formatRow('RowBinary', data)) FROM t_json_group_by_path_order WHERE id != 0 SETTINGS max_threads = 1;

DROP TABLE t_json_group_by_path_order;
