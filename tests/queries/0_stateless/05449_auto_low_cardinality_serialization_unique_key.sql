-- Tags: no-fasttest, no-ordinary-database, no-async-insert, no-replicated-database, no-shared-merge-tree

-- A column stored with automatic `LowCardinality` serialization is read as a dictionary-encoded column
-- while its type stays `String`. Inserting it into a `UNIQUE KEY` must encode the key from its values.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET enable_unique_key = 1;

DROP TABLE IF EXISTS t_auto_lc_uk_src;
DROP TABLE IF EXISTS t_auto_lc_uk_dst;

CREATE TABLE t_auto_lc_uk_src (k UInt64, s String STATISTICS(uniq))
ENGINE = MergeTree ORDER BY k
SETTINGS max_uniq_number_for_low_cardinality = 100, min_bytes_for_wide_part = 0;
INSERT INTO t_auto_lc_uk_src SELECT number, 'value_' || toString(number % 3) FROM numbers(3);

SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_uk_src' AND active AND column = 's';

CREATE TABLE t_auto_lc_uk_dst (s String) ENGINE = MergeTree ORDER BY s UNIQUE KEY (s);
INSERT INTO t_auto_lc_uk_dst SELECT s FROM t_auto_lc_uk_src SETTINGS max_threads = 1;
SELECT * FROM t_auto_lc_uk_dst ORDER BY s;

DROP TABLE t_auto_lc_uk_dst;
DROP TABLE t_auto_lc_uk_src;
