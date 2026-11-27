-- The stream of an empty `Tuple` is read by the path it is enumerated with, which is how a `v2` part finds its region.
-- A top-level empty `Tuple` column of the same part reads as before.
DROP TABLE IF EXISTS t_empty_tuple;
CREATE TABLE t_empty_tuple (id UInt64, json JSON(SHARED a Tuple(), SHARED b UInt32), t Tuple())
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, object_typed_shared_data_serialization_version = 'v2';

INSERT INTO t_empty_tuple SELECT number, toJSONString(map('b', number)), tuple() FROM numbers(3);

SELECT json.a, json.b FROM t_empty_tuple ORDER BY id;
SELECT json FROM t_empty_tuple ORDER BY id;
SELECT t FROM t_empty_tuple ORDER BY id;

DROP TABLE t_empty_tuple;
