-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- `make_distributed_plan = DEFAULT` is rejected in a query's own top-level SETTINGS (SELECT, a
-- UNION's trailing clause, after FORMAT, INSERT ... SELECT, EXPLAIN, CREATE ... AS SELECT), but
-- still works through `SET` and inside a nested subquery's own clause.

SELECT 1 SETTINGS make_distributed_plan = DEFAULT; -- { serverError INVALID_SETTING_VALUE }

SELECT 1 UNION ALL SELECT 2 SETTINGS make_distributed_plan = DEFAULT; -- { serverError INVALID_SETTING_VALUE }

SELECT 1 FORMAT Null SETTINGS make_distributed_plan = DEFAULT; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE IF EXISTS t_dp_default_rejected;
CREATE TABLE t_dp_default_rejected (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_dp_default_rejected SELECT 1 SETTINGS make_distributed_plan = DEFAULT; -- { serverError INVALID_SETTING_VALUE }

EXPLAIN PIPELINE SELECT 1 SETTINGS make_distributed_plan = DEFAULT; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE IF EXISTS t_dp_default_rejected_ctas;
CREATE TABLE t_dp_default_rejected_ctas ENGINE = Memory AS SELECT 1 SETTINGS make_distributed_plan = DEFAULT; -- { serverError INVALID_SETTING_VALUE }

SET make_distributed_plan = DEFAULT;

SELECT * FROM (SELECT 1 SETTINGS make_distributed_plan = DEFAULT);

SELECT getSetting('make_distributed_plan')
    SETTINGS distributed_plan_workers_num = 3, distributed_plan_execute_locally = 1, make_distributed_plan = 1;

DROP TABLE IF EXISTS t_dp_default_rejected_data;
CREATE TABLE t_dp_default_rejected_data (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_dp_default_rejected_data SELECT number FROM numbers(100000);

SELECT count() > 0
FROM (EXPLAIN PIPELINE SELECT sum(k) FROM t_dp_default_rejected_data
    SETTINGS distributed_plan_workers_num = 3, distributed_plan_execute_locally = 1, make_distributed_plan = 1, max_rows_to_group_by = 0)
WHERE explain LIKE '%ReadFromDistributedPlanSource%';

DROP TABLE t_dp_default_rejected;
-- Never created: the CREATE ... AS SELECT above is rejected before the table is created.
DROP TABLE IF EXISTS t_dp_default_rejected_ctas;
DROP TABLE t_dp_default_rejected_data;
