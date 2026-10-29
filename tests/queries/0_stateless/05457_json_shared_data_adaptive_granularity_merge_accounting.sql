-- The constant adaptive granularity of a merged part must be sized by the data a read decompresses, not
-- by the part's files. The dynamic shared data of JSON (ADVANCED serialization) keeps a second on-disk
-- copy of its values for whole-column reads; a whole-column read does not touch it. Counting both copies
-- made the merged part's granularity too small (too many marks). The merge-path granularity must stay
-- close to the insert-path granularity of the same rows, not roughly double it.
DROP TABLE IF EXISTS t_json_gran_ins;
DROP TABLE IF EXISTS t_json_gran_mrg;

CREATE TABLE t_json_gran_ins (x UInt64, json JSON(max_dynamic_paths = 0)) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 100000, index_granularity_bytes = 10240, use_const_adaptive_granularity = 1,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         object_shared_data_serialization_version = 'advanced',
         object_shared_data_serialization_version_for_zero_level_parts = 'advanced';
CREATE TABLE t_json_gran_mrg AS t_json_gran_ins;

-- max_dynamic_paths = 0 sends every path to shared data; the large value makes it dominate the column.
INSERT INTO t_json_gran_ins SELECT number, concat('{"a":"', repeat('y', 500), '"}') FROM numbers(10000);
INSERT INTO t_json_gran_mrg SELECT number, concat('{"a":"', repeat('y', 500), '"}') FROM numbers(5000);
INSERT INTO t_json_gran_mrg SELECT number + 5000, concat('{"a":"', repeat('y', 500), '"}') FROM numbers(5000);
OPTIMIZE TABLE t_json_gran_mrg FINAL;

-- One active Wide part each.
SELECT count(), min(part_type = 'Wide') FROM system.parts WHERE database = currentDatabase() AND table = 't_json_gran_ins' AND active;
SELECT count(), min(part_type = 'Wide') FROM system.parts WHERE database = currentDatabase() AND table = 't_json_gran_mrg' AND active;

-- Same rows round-trip through the merge.
SELECT count(), sum(json.a::String = repeat('y', 500)) FROM t_json_gran_mrg;

-- With the fix the merged part's marks are within 1.5x of the insert part's; without it they are ~2x.
SELECT 2 * (SELECT sum(marks) FROM system.parts WHERE database = currentDatabase() AND table = 't_json_gran_mrg' AND active)
    <= 3 * (SELECT sum(marks) FROM system.parts WHERE database = currentDatabase() AND table = 't_json_gran_ins' AND active);

DROP TABLE t_json_gran_ins;
DROP TABLE t_json_gran_mrg;
