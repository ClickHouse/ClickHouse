-- Tags: long, no-distributed-cache
-- long: a merge buffers about 80 MB of one path
-- no-distributed-cache: the temporary storage is the distributed cache, which the spiller does not use

-- A region larger than the reservation step of the temporary storage (64 MiB) goes to the temporary storage: the file is
-- reserved with room for it instead of closing the temporary storage, which would send every region to the part's
-- directory.

DROP TABLE IF EXISTS t_spill_large;

CREATE TABLE t_spill_large (x UInt64, json JSON(SHARED a String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, object_typed_shared_data_serialization_version = 'v2',
         serialization_spill_max_bytes_to_buffer = 73400320;
INSERT INTO t_spill_large SELECT number, concat('{"a":"', repeat(toString(number % 10), 1000), '"}') FROM numbers(40000);
INSERT INTO t_spill_large SELECT number, concat('{"a":"', repeat(toString(number % 10), 1000), '"}') FROM numbers(40000, 40000);
OPTIMIZE TABLE t_spill_large FINAL;

SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['SerializationSpilledSegments'] > 0, ProfileEvents['SerializationSpilledToPartDisk']
FROM system.part_log WHERE database = currentDatabase() AND table = 't_spill_large' AND event_type = 'MergeParts'
    AND part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_spill_large' AND active);
SELECT count(), sum(length(json.a)), sum(toUInt64(substring(json.a, 1, 1)) != x % 10) FROM t_spill_large;

DROP TABLE t_spill_large;
