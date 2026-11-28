-- An implicit min-max index over an ALIAS column has the same definition in two tables with the same
-- physical columns, but its files are built from the alias expression. Partition import must not let
-- index files built from one expression be used for another.

SET use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS same;

CREATE TABLE src (a UInt64, b UInt64 ALIAS a + 1) ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0, auto_statistics_types = '';
CREATE TABLE dst (a UInt64, b UInt64 ALIAS a + 1000000) ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0, auto_statistics_types = '';
CREATE TABLE same (a UInt64, b UInt64 ALIAS a + 1) ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0, auto_statistics_types = '';

INSERT INTO src SELECT number FROM numbers(100000);

ALTER TABLE dst ATTACH PARTITION ALL FROM src; -- { serverError BAD_ARGUMENTS }
ALTER TABLE dst REPLACE PARTITION tuple() FROM src; -- { serverError BAD_ARGUMENTS }

-- The same alias expression is accepted.
ALTER TABLE same ATTACH PARTITION ALL FROM src;
SELECT count() FROM same WHERE b = 1001;
SELECT count() FROM same WHERE b = 1001 SETTINGS use_skip_indexes = 0;

DROP TABLE src;
DROP TABLE dst;
DROP TABLE same;
