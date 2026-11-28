-- Tags: no-old-analyzer

-- A column stored with automatic `LowCardinality` serialization is read as a dictionary-encoded column
-- while its type stays `String`. Lazy materialization must accept it when it pads the unmatched rows of an
-- outer join.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_auto_lc_lazy_l;
DROP TABLE IF EXISTS t_auto_lc_lazy_r;

CREATE TABLE t_auto_lc_lazy_l (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_auto_lc_lazy_r (k UInt64, s String STATISTICS(uniq))
ENGINE = MergeTree ORDER BY k
SETTINGS max_uniq_number_for_low_cardinality = 100, min_bytes_for_wide_part = 0;

INSERT INTO t_auto_lc_lazy_l SELECT number FROM numbers(10000);
INSERT INTO t_auto_lc_lazy_r SELECT number * 2, 'value' FROM numbers(5000);

SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_lazy_r' AND active AND column = 's';

SELECT 'lazy join';
SELECT l.k, r.s
FROM t_auto_lc_lazy_l AS l LEFT JOIN t_auto_lc_lazy_r AS r ON l.k = r.k
ORDER BY l.k LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 1,
         query_plan_lazy_materialization_for_join = 1,
         query_plan_max_limit_for_lazy_materialization = 10,
         enable_join_runtime_filters = 0,
         query_plan_join_swap_table = 0,
         query_plan_optimize_join_order_limit = 0,
         join_algorithm = 'hash';

SELECT 'no lazy join';
SELECT l.k, r.s
FROM t_auto_lc_lazy_l AS l LEFT JOIN t_auto_lc_lazy_r AS r ON l.k = r.k
ORDER BY l.k LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 0,
         query_plan_join_swap_table = 0,
         join_algorithm = 'hash';

DROP TABLE t_auto_lc_lazy_l;

DROP TABLE t_auto_lc_lazy_r;
