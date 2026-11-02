-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database, no-shared-merge-tree: their block numbers do not restart, so no part path is reused.

-- TRUNCATE + DETACH/ATTACH restarts the block numbers, so the next INSERT writes part `all_1_1_0` again, into the
-- directory of the removed part. The caches keyed by a file path must not return the data of the removed part.

-- Only the caches under test are used.
SET use_query_condition_cache = 0, use_skip_indexes = 1, min_bytes_to_use_mmap_io = 0, use_page_cache_for_local_disks = 0, use_uncompressed_cache = 0;
SET use_text_index_tokens_cache = 1, use_text_index_header_cache = 1, use_text_index_postings_cache = 1;
SET merge_tree_max_rows_to_use_cache = 1000000000, merge_tree_max_bytes_to_use_cache = 1000000000000;

-- Text index caches: wide and compact parts.

CREATE TABLE text_wide (k UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', old_parts_lifetime = 0, min_bytes_for_wide_part = 0;
INSERT INTO text_wide SELECT number, if(number < 10, 'apple', 'pear') FROM numbers(1000);
SELECT count() FROM text_wide WHERE hasToken(s, 'apple');
TRUNCATE TABLE text_wide;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'text_wide';
DETACH TABLE text_wide;
ATTACH TABLE text_wide;
INSERT INTO text_wide SELECT number, if(number = 999, 'apple', 'pear') FROM numbers(1000);
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'text_wide' AND active;
SELECT count() FROM text_wide WHERE hasToken(s, 'apple');
SELECT groupArray(k) FROM text_wide WHERE hasToken(s, 'apple');

CREATE TABLE text_compact (k UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', old_parts_lifetime = 0, min_bytes_for_wide_part = 1000000000;
INSERT INTO text_compact SELECT number, if(number < 10, 'apple', 'pear') FROM numbers(1000);
SELECT count() FROM text_compact WHERE hasToken(s, 'apple');
TRUNCATE TABLE text_compact;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'text_compact';
DETACH TABLE text_compact;
ATTACH TABLE text_compact;
INSERT INTO text_compact SELECT number, if(number = 999, 'apple', 'pear') FROM numbers(1000);
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'text_compact' AND active;
SELECT count() FROM text_compact WHERE hasToken(s, 'apple');
SELECT groupArray(k) FROM text_compact WHERE hasToken(s, 'apple');

-- Uncompressed cache: wide and compact parts, a new part of the same size and a larger one.

CREATE TABLE uc_wide (a UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', old_parts_lifetime = 0, min_bytes_for_wide_part = 0;
INSERT INTO uc_wide SELECT number FROM numbers(100);
SELECT sum(a) FROM uc_wide SETTINGS use_uncompressed_cache = 1;
TRUNCATE TABLE uc_wide;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uc_wide';
DETACH TABLE uc_wide;
ATTACH TABLE uc_wide;
INSERT INTO uc_wide SELECT number * 2 FROM numbers(100);
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'uc_wide' AND active;
SELECT sum(a) FROM uc_wide SETTINGS use_uncompressed_cache = 1;

CREATE TABLE uc_wide_larger (a UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', old_parts_lifetime = 0, min_bytes_for_wide_part = 0;
INSERT INTO uc_wide_larger SELECT number FROM numbers(100);
SELECT sum(a) FROM uc_wide_larger SETTINGS use_uncompressed_cache = 1;
TRUNCATE TABLE uc_wide_larger;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uc_wide_larger';
DETACH TABLE uc_wide_larger;
ATTACH TABLE uc_wide_larger;
INSERT INTO uc_wide_larger SELECT number FROM numbers(1000);
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'uc_wide_larger' AND active;
SELECT sum(a) FROM uc_wide_larger SETTINGS use_uncompressed_cache = 1;

CREATE TABLE uc_compact (a UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', old_parts_lifetime = 0, min_bytes_for_wide_part = 1000000000;
INSERT INTO uc_compact SELECT number FROM numbers(100);
SELECT sum(a) FROM uc_compact SETTINGS use_uncompressed_cache = 1;
TRUNCATE TABLE uc_compact;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uc_compact';
DETACH TABLE uc_compact;
ATTACH TABLE uc_compact;
INSERT INTO uc_compact SELECT number * 2 FROM numbers(100);
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'uc_compact' AND active;
SELECT sum(a) FROM uc_compact SETTINGS use_uncompressed_cache = 1;

CREATE TABLE uc_compact_larger (a UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', old_parts_lifetime = 0, min_bytes_for_wide_part = 1000000000;
INSERT INTO uc_compact_larger SELECT number FROM numbers(100);
SELECT sum(a) FROM uc_compact_larger SETTINGS use_uncompressed_cache = 1;
TRUNCATE TABLE uc_compact_larger;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uc_compact_larger';
DETACH TABLE uc_compact_larger;
ATTACH TABLE uc_compact_larger;
INSERT INTO uc_compact_larger SELECT number FROM numbers(1000);
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'uc_compact_larger' AND active;
SELECT sum(a) FROM uc_compact_larger SETTINGS use_uncompressed_cache = 1;

-- Index uncompressed cache (minmax skip index): wide and compact parts, the index in `skp_idx.packed` and in its own file.

CREATE TABLE mm_wide_packed (k UInt64, a UInt64, INDEX mm a TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', old_parts_lifetime = 0, min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 1048576;
INSERT INTO mm_wide_packed SELECT number, number FROM numbers(1000);
SELECT count() FROM mm_wide_packed WHERE a = 5;
TRUNCATE TABLE mm_wide_packed;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'mm_wide_packed';
DETACH TABLE mm_wide_packed;
ATTACH TABLE mm_wide_packed;
INSERT INTO mm_wide_packed SELECT number, 999 - number FROM numbers(1000);
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'mm_wide_packed' AND active;
SELECT count() FROM mm_wide_packed WHERE a = 5;

CREATE TABLE mm_wide_files (k UInt64, a UInt64, INDEX mm a TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', old_parts_lifetime = 0, min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0;
INSERT INTO mm_wide_files SELECT number, number FROM numbers(1000);
SELECT count() FROM mm_wide_files WHERE a = 5;
TRUNCATE TABLE mm_wide_files;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'mm_wide_files';
DETACH TABLE mm_wide_files;
ATTACH TABLE mm_wide_files;
INSERT INTO mm_wide_files SELECT number, 999 - number FROM numbers(1000);
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'mm_wide_files' AND active;
SELECT count() FROM mm_wide_files WHERE a = 5;

CREATE TABLE mm_compact_packed (k UInt64, a UInt64, INDEX mm a TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', old_parts_lifetime = 0, min_bytes_for_wide_part = 1000000000, packed_skip_index_max_bytes = 1048576;
INSERT INTO mm_compact_packed SELECT number, number FROM numbers(1000);
SELECT count() FROM mm_compact_packed WHERE a = 5;
TRUNCATE TABLE mm_compact_packed;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'mm_compact_packed';
DETACH TABLE mm_compact_packed;
ATTACH TABLE mm_compact_packed;
INSERT INTO mm_compact_packed SELECT number, 999 - number FROM numbers(1000);
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'mm_compact_packed' AND active;
SELECT count() FROM mm_compact_packed WHERE a = 5;

CREATE TABLE mm_compact_files (k UInt64, a UInt64, INDEX mm a TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', old_parts_lifetime = 0, min_bytes_for_wide_part = 1000000000, packed_skip_index_max_bytes = 0;
INSERT INTO mm_compact_files SELECT number, number FROM numbers(1000);
SELECT count() FROM mm_compact_files WHERE a = 5;
TRUNCATE TABLE mm_compact_files;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'mm_compact_files';
DETACH TABLE mm_compact_files;
ATTACH TABLE mm_compact_files;
INSERT INTO mm_compact_files SELECT number, 999 - number FROM numbers(1000);
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'mm_compact_files' AND active;
SELECT count() FROM mm_compact_files WHERE a = 5;
