-- Tags: long, no-distributed-cache
-- long: each merge spills about 100 MB of one path
-- no-distributed-cache: the temporary storage is the distributed cache, which the spiller does not use

-- A merge spilling more than the reservation step of the temporary storage (64 MiB) keeps one file and grows its
-- reservation. With a limit of 70 MiB the file grows only to the limit, and the rest goes to the part's disk.
-- The values do not compress, so that the file fills its first step, and the granules are small, so that the merge
-- spills many small segments.

DROP TABLE IF EXISTS t_steps;

CREATE TABLE t_steps (x UInt64, json JSON(SHARED a String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 0, merge_max_block_size = 8192,
         index_granularity = 1024, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 1048576;

CREATE TEMPORARY TABLE src AS SELECT number AS x, hex(randomString(500)) AS a FROM numbers(100000);

INSERT INTO t_steps SELECT x, concat('{"a":"', a, '"}') FROM src WHERE x < 50000;
INSERT INTO t_steps SELECT x, concat('{"a":"', a, '"}') FROM src WHERE x >= 50000;
OPTIMIZE TABLE t_steps FINAL;

ALTER TABLE t_steps MODIFY SETTING serialization_spill_max_temporary_data_size = 73400320;
OPTIMIZE TABLE t_steps FINAL;

SYSTEM FLUSH LOGS part_log;

SELECT part_name, ProfileEvents['SerializationSpilledSegments'] > 0, ProfileEvents['SerializationSpillFiles'], ProfileEvents['SerializationSpilledToPartDisk'] > 0
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_steps' AND event_type = 'MergeParts'
ORDER BY part_name;

SELECT count(), (SELECT sum(cityHash64(x, toString(json.a))) FROM t_steps) = (SELECT sum(cityHash64(x, a)) FROM src) FROM t_steps;

DROP TABLE t_steps;
