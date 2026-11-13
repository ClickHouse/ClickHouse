-- A `Memory` table filled from a table with automatic `LowCardinality` serialization keeps full `String`
-- columns, so subcolumns (`s.size` after `optimize_functions_to_subcolumns`) are resolved transparently.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_auto_lc_memory;
DROP TABLE IF EXISTS t_auto_lc_memory_compressed;
DROP TABLE IF EXISTS t_auto_lc_source;

CREATE TABLE t_auto_lc_source
(
    id UInt64,
    s String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 1,
    min_bytes_for_wide_part = 0,
    auto_statistics_types = 'basic';

INSERT INTO t_auto_lc_source SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);

SELECT 'source kind';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_source' AND active AND column = 's';

CREATE TABLE t_auto_lc_memory (id UInt64, s String) ENGINE = Memory;
INSERT INTO t_auto_lc_memory SELECT id, s FROM t_auto_lc_source;

SELECT 'memory';
SELECT sum(s.size), sum(length(s)), countIf(notEmpty(s)), countIf(empty(s)), uniqExact(s) FROM t_auto_lc_memory;

CREATE TABLE t_auto_lc_memory_compressed (id UInt64, s String) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_auto_lc_memory_compressed SELECT id, s FROM t_auto_lc_source;

SELECT 'memory compressed';
SELECT sum(s.size), sum(length(s)), countIf(notEmpty(s)), countIf(empty(s)), uniqExact(s) FROM t_auto_lc_memory_compressed;

DROP TABLE t_auto_lc_memory;
DROP TABLE t_auto_lc_memory_compressed;
DROP TABLE t_auto_lc_source;
