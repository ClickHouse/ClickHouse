-- Dropping an ALIAS column removes its implicit min-max index from the metadata without a mutation, so the
-- parts keep the index files built from the old alias expression. Partition import into a table that declares
-- an index with the same name must not let the destination use these files for its own expression.

SET use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS fresh;

CREATE TABLE src (a UInt64, b UInt64 ALIAS a + 1) ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0, auto_statistics_types = '';
CREATE TABLE dst (a UInt64, b UInt64 ALIAS a + 1000000) ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0, auto_statistics_types = '';

INSERT INTO src SELECT number FROM numbers(100000);
ALTER TABLE src DROP COLUMN b;

ALTER TABLE dst ATTACH PARTITION ALL FROM src; -- { serverError BAD_ARGUMENTS }
ALTER TABLE dst REPLACE PARTITION tuple() FROM src; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM dst;

-- Parts without such files are accepted, and the destination-only index is absent from them.
CREATE TABLE fresh (a UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0, auto_statistics_types = '';
INSERT INTO fresh SELECT number FROM numbers(100000);
ALTER TABLE dst ATTACH PARTITION ALL FROM fresh;
SELECT count() FROM dst WHERE b = 1000001;
SELECT count() FROM dst WHERE b = 1000001 SETTINGS use_skip_indexes = 0;

DROP TABLE src;
DROP TABLE dst;
DROP TABLE fresh;

-- Only the imported parts are checked: stale files in another partition of the source do not reject the import
-- of a clean partition, while the stale partition itself is still rejected.
DROP TABLE IF EXISTS src_p;
DROP TABLE IF EXISTS dst_p;
DROP TABLE IF EXISTS dst_p_move;

CREATE TABLE src_p (p UInt8, a UInt64, b UInt64 ALIAS a + 1) ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0, auto_statistics_types = '';
CREATE TABLE dst_p (p UInt8, a UInt64, b UInt64 ALIAS a + 1000000) ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0, auto_statistics_types = '';
CREATE TABLE dst_p_move AS dst_p;

INSERT INTO src_p SELECT 0, number FROM numbers(100000);
ALTER TABLE src_p DROP COLUMN b;
INSERT INTO src_p SELECT 1, number FROM numbers(100000);

ALTER TABLE dst_p ATTACH PARTITION 0 FROM src_p; -- { serverError BAD_ARGUMENTS }
ALTER TABLE dst_p REPLACE PARTITION 0 FROM src_p; -- { serverError BAD_ARGUMENTS }
ALTER TABLE src_p MOVE PARTITION 0 TO TABLE dst_p_move; -- { serverError BAD_ARGUMENTS }

ALTER TABLE dst_p ATTACH PARTITION 1 FROM src_p;
SELECT count() FROM dst_p WHERE b = 1000001;
SELECT count() FROM dst_p WHERE b = 1000001 SETTINGS use_skip_indexes = 0;

ALTER TABLE src_p MOVE PARTITION 1 TO TABLE dst_p_move;
SELECT count() FROM dst_p_move WHERE b = 1000001;
SELECT count() FROM dst_p_move WHERE b = 1000001 SETTINGS use_skip_indexes = 0;

DROP TABLE src_p;
DROP TABLE dst_p;
DROP TABLE dst_p_move;
