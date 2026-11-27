-- Tags: long, no-parallel, no-fasttest, no-distributed-cache
-- long: the merge spills about 100 MB of one path
-- no-parallel: the failpoint is global and would refuse a reservation of the spills of the other tests
-- no-fasttest: the failpoint is global and must not fire in the concurrently running fast tests
-- no-distributed-cache: the temporary storage is the distributed cache, which the spiller does not use

-- A refused extension of the reservation of a spill file finishes the file, which keeps the segments spilled to it,
-- and the next segments go to a new file of the temporary storage. The values do not compress, so that the file fills
-- its first reservation step (64 MiB), and the granules are small, so that the merge spills many small segments.

SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_reserve_refused;

CREATE TABLE t_reserve_refused (x UInt64, json JSON(SHARED a String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 0, merge_max_block_size = 8192,
         index_granularity = 1024, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 1048576;

CREATE TEMPORARY TABLE src AS SELECT number AS x, hex(randomString(500)) AS a FROM numbers(100000);

INSERT INTO t_reserve_refused SELECT x, concat('{"a":"', a, '"}') FROM src WHERE x < 50000;
INSERT INTO t_reserve_refused SELECT x, concat('{"a":"', a, '"}') FROM src WHERE x >= 50000;

SYSTEM ENABLE FAILPOINT serialization_spill_reserve_more_refused;
OPTIMIZE TABLE t_reserve_refused FINAL;
SYSTEM DISABLE FAILPOINT serialization_spill_reserve_more_refused;

SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['SerializationSpilledSegments'] > 0, ProfileEvents['SerializationSpillFiles'], ProfileEvents['SerializationSpilledToPartDisk']
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_reserve_refused' AND event_type = 'MergeParts'
    AND part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_reserve_refused' AND active);

SELECT count(), (SELECT sum(cityHash64(x, toString(json.a))) FROM t_reserve_refused) = (SELECT sum(cityHash64(x, a)) FROM src)
FROM t_reserve_refused;
CHECK TABLE t_reserve_refused;

DROP TABLE t_reserve_refused;
