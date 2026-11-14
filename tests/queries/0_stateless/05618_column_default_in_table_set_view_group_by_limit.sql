-- A `DEFAULT p IN view` of a column missing in a part builds its set by reading the view. The limits of the reading
-- query, such as `max_rows_to_group_by` for an aggregating view, have to bound that reading as they bound `p IN view`.

-- Aggregation in order does not check `max_rows_to_group_by`.
SET optimize_aggregation_in_order = 0;

DROP TABLE IF EXISTS t_default_set_group_by;
DROP VIEW IF EXISTS grouped_keys_default_set;
DROP TABLE IF EXISTS keys_default_set_group_by;

CREATE TABLE keys_default_set_group_by (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO keys_default_set_group_by SELECT number FROM numbers(100);
CREATE VIEW grouped_keys_default_set AS SELECT p FROM keys_default_set_group_by GROUP BY p;

CREATE TABLE t_default_set_group_by (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO t_default_set_group_by VALUES (1), (2);
ALTER TABLE t_default_set_group_by ADD COLUMN f UInt8 DEFAULT p IN grouped_keys_default_set;

SELECT sum(f) FROM t_default_set_group_by;
SELECT sum(p IN grouped_keys_default_set) FROM t_default_set_group_by SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }
SELECT sum(f) FROM t_default_set_group_by SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_default_set_group_by;
DROP VIEW grouped_keys_default_set;
DROP TABLE keys_default_set_group_by;
