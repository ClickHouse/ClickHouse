-- Automatic `LowCardinality` serialization: the subcolumn rewrite guard looks up a part's serialization
-- infos under the old name of a column with a pending `RENAME COLUMN`, and `WITH FILL ... INTERPOLATE`
-- materializes the encoded column when no sorting transform is installed before it. The radix
-- partitioning of a single `uniqExact` routes the values of the encoded column.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_auto_lc_pending_rename;
DROP TABLE IF EXISTS t_auto_lc_fill;

CREATE TABLE t_auto_lc_pending_rename
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 1,
    min_bytes_for_wide_part = 0,
    auto_statistics_types = 'basic';

INSERT INTO t_auto_lc_pending_rename SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);

SELECT 'kind';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_pending_rename' AND active AND column = 'lc';

-- With the threshold at zero only the encoded parts make the guard skip the rewrite. The rename
-- stays pending because merges (and so mutations) are stopped.
SYSTEM STOP MERGES t_auto_lc_pending_rename;
ALTER TABLE t_auto_lc_pending_rename MODIFY SETTING max_uniq_number_for_low_cardinality = 0;
ALTER TABLE t_auto_lc_pending_rename RENAME COLUMN lc TO lc2 SETTINGS alter_sync = 0;

SELECT 'pending rename';
SELECT count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(lc2) FROM t_auto_lc_pending_rename SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1) WHERE explain LIKE '%lc2.size%';
SELECT sum(length(lc2)) FROM t_auto_lc_pending_rename SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1;

SYSTEM START MERGES t_auto_lc_pending_rename;
DROP TABLE t_auto_lc_pending_rename;

CREATE TABLE t_auto_lc_fill
(
    id UInt64,
    s String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 100,
    ratio_of_defaults_for_sparse_serialization = 1,
    min_bytes_for_wide_part = 0,
    auto_statistics_types = 'basic';

INSERT INTO t_auto_lc_fill VALUES (0, 'x'), (2, 'x'), (5, 'y');

SELECT 'kind';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_fill' AND active AND column = 's';

SELECT 'fill';
SELECT id, s FROM t_auto_lc_fill ORDER BY id WITH FILL INTERPOLATE (s)
SETTINGS max_threads = 1, optimize_read_in_order = 1;

SELECT 'fill with expression';
SELECT id, s FROM t_auto_lc_fill ORDER BY id WITH FILL INTERPOLATE (s AS s || '+')
SETTINGS max_threads = 1, optimize_read_in_order = 1;

SELECT 'uniqExact';
SELECT uniqExact(s), count(DISTINCT s) FROM t_auto_lc_fill SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT DISTINCT uniqExact(s) IGNORE NULLS FROM t_auto_lc_fill SETTINGS optimize_uniq_exact_radix_partitioning = 1;

DROP TABLE t_auto_lc_fill;
