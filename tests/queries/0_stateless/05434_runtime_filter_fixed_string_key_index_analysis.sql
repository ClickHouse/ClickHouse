-- The join keys and the runtime filters remove the trailing zero bytes of a `FixedString` key converted to `String`,
-- whatever `cast_fixed_string_to_string_strip_trailing_zeros` is. That holds for the primary key analysis by the
-- runtime filter, and for the runtime filters transported between the stages of a distributed plan.

DROP TABLE IF EXISTS t_rf_fs;
DROP TABLE IF EXISTS t_rf_s;

CREATE TABLE t_rf_fs (k FixedString(2)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
CREATE TABLE t_rf_s (k String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_rf_fs VALUES ('a'), ('b');
INSERT INTO t_rf_s VALUES ('a'), ('b');

SET query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;

SELECT count() FROM t_rf_fs JOIN t_rf_s USING k
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 0, enable_join_runtime_filters_index_analysis = 1;
SELECT count() FROM t_rf_fs JOIN t_rf_s USING k
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1, enable_join_runtime_filters_index_analysis = 1;

DROP TABLE t_rf_fs;
DROP TABLE t_rf_s;

DROP TABLE IF EXISTS t_rf_probe;
DROP TABLE IF EXISTS t_rf_build;

CREATE TABLE t_rf_probe (k String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_build (k FixedString(8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rf_probe SELECT toString(number) FROM numbers(100000);
INSERT INTO t_rf_build SELECT toString(number * 100) FROM numbers(1000);

SELECT count() FROM t_rf_probe AS p JOIN t_rf_build AS b ON p.k = b.k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_join_runtime_filters = 0;
SELECT count() FROM t_rf_probe AS p JOIN t_rf_build AS b ON p.k = b.k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_join_runtime_filters = 1;
SELECT count() FROM t_rf_probe AS p JOIN t_rf_build AS b ON p.k = b.k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_join_runtime_filters = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;

DROP TABLE t_rf_probe;
DROP TABLE t_rf_build;
