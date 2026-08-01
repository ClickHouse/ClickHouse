-- Projections with the `_part_offset` column are merged (not rebuilt) in merges that only
-- drop rows: projection rows of dropped parent rows are filtered out and `_parent_part_offset`
-- is remapped through the offset mapping.

SET enable_analyzer = 1;
SET lightweight_deletes_sync = 2;

SELECT 'ReplacingMergeTree';

DROP TABLE IF EXISTS t_proj_replacing;

CREATE TABLE t_proj_replacing
(
    id UInt64,
    version UInt64,
    v UInt64,
    PROJECTION p (SELECT id, v, _part_offset ORDER BY v)
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1, deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO t_proj_replacing SELECT number, 1, number * 7 FROM numbers(10000);
INSERT INTO t_proj_replacing SELECT number, 2, number * 7 + 1 FROM numbers(0, 10000, 3);

OPTIMIZE TABLE t_proj_replacing FINAL;

SELECT count() FROM t_proj_replacing;
SELECT count() FROM mergeTreeProjection(currentDatabase(), t_proj_replacing, p);
SELECT sum(l._part_offset = r._parent_part_offset) FROM t_proj_replacing l JOIN mergeTreeProjection(currentDatabase(), t_proj_replacing, p) r USING (id);

SELECT 'query through projection';
SELECT id, v FROM t_proj_replacing WHERE v = 4200 ORDER BY id; -- replaced row, no result
SELECT id, v FROM t_proj_replacing WHERE v = 4201 ORDER BY id;
SELECT id, v FROM t_proj_replacing WHERE v = 4207 ORDER BY id;

SYSTEM FLUSH LOGS part_log;

SELECT 'projections merged, not rebuilt';
SELECT ProfileEvents['MergedProjections'], ProfileEvents['RebuiltProjections'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_replacing' AND event_type = 'MergeParts' AND error = 0 AND part_name NOT LIKE '%.proj'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_proj_replacing;

SELECT 'lightweight delete';

DROP TABLE IF EXISTS t_proj_lwd;

CREATE TABLE t_proj_lwd
(
    id UInt64,
    v UInt64,
    PROJECTION p (SELECT id, v, _part_offset ORDER BY v)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1, lightweight_mutation_projection_mode = 'rebuild';

INSERT INTO t_proj_lwd SELECT number, number * 7 FROM numbers(10000);
INSERT INTO t_proj_lwd SELECT number, number * 7 FROM numbers(10000, 10000);

DELETE FROM t_proj_lwd WHERE id % 4 = 0;

OPTIMIZE TABLE t_proj_lwd FINAL;

SELECT count() FROM t_proj_lwd;
SELECT count() FROM mergeTreeProjection(currentDatabase(), t_proj_lwd, p);
SELECT sum(l._part_offset = r._parent_part_offset) FROM t_proj_lwd l JOIN mergeTreeProjection(currentDatabase(), t_proj_lwd, p) r USING (id);

SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['MergedProjections'], ProfileEvents['RebuiltProjections'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_lwd' AND event_type = 'MergeParts' AND error = 0 AND part_name NOT LIKE '%.proj'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_proj_lwd;

SELECT 'TTL DELETE';

DROP TABLE IF EXISTS t_proj_ttl;

CREATE TABLE t_proj_ttl
(
    id UInt64,
    d DateTime,
    v UInt64,
    PROJECTION p (SELECT id, v, _part_offset ORDER BY v)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

INSERT INTO t_proj_ttl SELECT number, if(number % 3 = 0, toDateTime('2001-01-01 00:00:00'), toDateTime('2101-01-01 00:00:00')), number * 7 FROM numbers(5000);
INSERT INTO t_proj_ttl SELECT number, if(number % 3 = 0, toDateTime('2001-01-01 00:00:00'), toDateTime('2101-01-01 00:00:00')), number * 7 FROM numbers(5000, 5000);

ALTER TABLE t_proj_ttl MODIFY TTL d SETTINGS materialize_ttl_after_modify = 0;

OPTIMIZE TABLE t_proj_ttl FINAL;

SELECT count() FROM t_proj_ttl;
SELECT count() FROM mergeTreeProjection(currentDatabase(), t_proj_ttl, p);
SELECT sum(l._part_offset = r._parent_part_offset) FROM t_proj_ttl l JOIN mergeTreeProjection(currentDatabase(), t_proj_ttl, p) r USING (id);

SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['MergedProjections'], ProfileEvents['RebuiltProjections'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_ttl' AND event_type = 'MergeParts' AND error = 0 AND part_name NOT LIKE '%.proj'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_proj_ttl;

SELECT 'OPTIMIZE DEDUPLICATE BY columns not covered by the projection';

DROP TABLE IF EXISTS t_proj_dedup;

CREATE TABLE t_proj_dedup
(
    id UInt64,
    v UInt64,
    extra String,
    PROJECTION p (SELECT id, _part_offset ORDER BY id)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1, deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO t_proj_dedup SELECT number, number, 'a' FROM numbers(3000);
INSERT INTO t_proj_dedup SELECT number, number, 'b' FROM numbers(0, 3000, 2);

-- Deduplication of parent rows must not be re-applied to rows of the merged projection,
-- otherwise this would fail: the projection has no columns `v` and `extra`.
OPTIMIZE TABLE t_proj_dedup FINAL DEDUPLICATE BY id, v;

SELECT count() FROM t_proj_dedup;
SELECT count() FROM mergeTreeProjection(currentDatabase(), t_proj_dedup, p);
SELECT sum(l._part_offset = r._parent_part_offset) FROM t_proj_dedup l JOIN mergeTreeProjection(currentDatabase(), t_proj_dedup, p) r USING (id);

SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['MergedProjections'], ProfileEvents['RebuiltProjections'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_dedup' AND event_type = 'MergeParts' AND error = 0 AND part_name NOT LIKE '%.proj'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_proj_dedup;
