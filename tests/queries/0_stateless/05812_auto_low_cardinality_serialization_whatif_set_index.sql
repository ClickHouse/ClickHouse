-- Tags: no-replicated-database
-- no-replicated-database: hypothetical indexes are session-scoped and not replicated

-- An empirical `EXPLAIN WHATIF` reads the parts with its own pipeline executor and feeds the blocks to the
-- aggregator of a hypothetical index. A `set` index copies the column into a full `String` column, so the
-- automatic `LowCardinality` encoding must be materialized before it. The estimate must be the same as for
-- a table stored with the default serialization.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_auto_lc_whatif;
DROP TABLE IF EXISTS t_auto_lc_whatif_default;

CREATE TABLE t_auto_lc_whatif (id UInt64, s String STATISTICS(uniq))
ENGINE = MergeTree ORDER BY id
SETTINGS max_uniq_number_for_low_cardinality = 100, ratio_of_defaults_for_sparse_serialization = 1,
    min_bytes_for_wide_part = 0, index_granularity = 100, index_granularity_bytes = 0, auto_statistics_types = '';

CREATE TABLE t_auto_lc_whatif_default (id UInt64, s String STATISTICS(uniq))
ENGINE = MergeTree ORDER BY id
SETTINGS max_uniq_number_for_low_cardinality = 0, ratio_of_defaults_for_sparse_serialization = 1,
    min_bytes_for_wide_part = 0, index_granularity = 100, index_granularity_bytes = 0, auto_statistics_types = '';

-- The first five granules hold only 'x', the last five only 'y'.
INSERT INTO t_auto_lc_whatif SELECT number, if(number < 500, 'x', 'y') FROM numbers(1000);
INSERT INTO t_auto_lc_whatif_default SELECT number, if(number < 500, 'x', 'y') FROM numbers(1000);

SELECT table, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table LIKE 't_auto_lc_whatif%' AND column = 's' AND active
ORDER BY table;

CREATE HYPOTHETICAL INDEX idx_s ON t_auto_lc_whatif (s) TYPE set(100) GRANULARITY 1;
CREATE HYPOTHETICAL INDEX idx_s ON t_auto_lc_whatif_default (s) TYPE set(100) GRANULARITY 1;

SELECT replaceRegexpAll(trim(explain), ' +', ' ') AS line
FROM (EXPLAIN WHATIF SELECT * FROM t_auto_lc_whatif WHERE s = 'x')
WHERE explain LIKE '%skip_ratio:%' OR explain LIKE '%source:%'
ORDER BY line;

SELECT replaceRegexpAll(trim(explain), ' +', ' ') AS line
FROM (EXPLAIN WHATIF SELECT * FROM t_auto_lc_whatif_default WHERE s = 'x')
WHERE explain LIKE '%skip_ratio:%' OR explain LIKE '%source:%'
ORDER BY line;

DROP TABLE t_auto_lc_whatif;
DROP TABLE t_auto_lc_whatif_default;
