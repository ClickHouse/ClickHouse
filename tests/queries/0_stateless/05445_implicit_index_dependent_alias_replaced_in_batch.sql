-- The implicit index of an expression alias is built over its expanded expression. When an `ALTER` replaces
-- an alias that another alias reaches (`b` for `c ALIAS b + 1`), the index files of `c` in the existing parts
-- are stale even if `c` is re-added with the same definition, so its implicit index must not be re-created.

SET use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_dependent;

CREATE TABLE t_dependent (a UInt64, b UInt64 ALIAS a + 1, c UInt64 ALIAS b + 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_dependent SELECT number FROM numbers(100000);
ALTER TABLE t_dependent
    DROP COLUMN c,
    DROP COLUMN b,
    ADD COLUMN b UInt64 ALIAS a + 1000000,
    ADD COLUMN c UInt64 ALIAS b + 1;
SELECT count() FROM t_dependent WHERE c = 1000001;
SELECT count() FROM t_dependent WHERE c = 1000001 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_dependent' ORDER BY name;

DROP TABLE t_dependent;
