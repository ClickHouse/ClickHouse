-- Tags: no-fasttest
-- no-fasttest: needs the Parquet format

-- Planning `ORDER BY ... LIMIT` over a Parquet file checks whether the defaults of its columns are deterministic, to decide
-- if the top-K filter can be used. That only inspects the defaults: a default `p IN table` must not read the table there.
-- Here the file supplies every value of `f`, so the default is never evaluated, and the view on its right-hand side,
-- which throws when read, is not read with the top-K filter either enabled or disabled.

DROP VIEW IF EXISTS v_default_set_top_k_rhs;
CREATE VIEW v_default_set_top_k_rhs AS SELECT throwIf(number >= 0, 'The right-hand side is read') + number AS p FROM numbers(3);

INSERT INTO FUNCTION file(currentDatabase() || '_05898_full.parquet', 'Parquet', 'p UInt64, f UInt8')
    SELECT number, number % 2 FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05898_partial.parquet', 'Parquet', 'p UInt64')
    SELECT number FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 1;

SELECT p, f FROM file(currentDatabase() || '_05898_full.parquet', 'Parquet', 'p UInt64, f UInt8 DEFAULT p IN v_default_set_top_k_rhs')
ORDER BY p DESC LIMIT 1
SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000;

SELECT p, f FROM file(currentDatabase() || '_05898_full.parquet', 'Parquet', 'p UInt64, f UInt8 DEFAULT p IN v_default_set_top_k_rhs')
ORDER BY p DESC LIMIT 1
SETTINGS use_top_k_dynamic_filtering = 0;

-- When the file lacks `f`, the default is evaluated and the view is read.
SELECT p, f FROM file(currentDatabase() || '_05898_partial.parquet', 'Parquet', 'p UInt64, f UInt8 DEFAULT p IN v_default_set_top_k_rhs')
ORDER BY p DESC LIMIT 1
SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

DROP VIEW v_default_set_top_k_rhs;
