-- The substreams cache key of a SHARED path must differ from the keys of the typed paths of the same column whatever
-- the paths are called: a whole-column read decodes the typed paths first, and a key shared with one of them would hand
-- that path's values to the SHARED path.
DROP TABLE IF EXISTS t_collision;
DROP TABLE IF EXISTS t_collision_compact;
DROP TABLE IF EXISTS t_collision_types;

CREATE TABLE t_collision (j JSON(SHARED foo UInt64, SHARED header UInt64, object_typed_shared_data JSON(foo UInt64, shared_paths JSON(foo UInt64)), object_paths JSON(foo UInt64)))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_collision_compact (j JSON(SHARED foo UInt64, SHARED header UInt64, object_typed_shared_data JSON(foo UInt64, shared_paths JSON(foo UInt64)), object_paths JSON(foo UInt64)))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_collision VALUES ('{"foo":1,"header":2,"object_typed_shared_data":{"foo":100,"shared_paths":{"foo":200}},"object_paths":{"foo":300}}');
INSERT INTO t_collision_compact SELECT * FROM t_collision;

SELECT j FROM t_collision;
SELECT j.foo, j.header, j.object_typed_shared_data.foo, j.object_typed_shared_data.shared_paths.foo, j.object_paths.foo FROM t_collision;
SELECT j FROM t_collision_compact;
SELECT j.foo, j.header, j.object_typed_shared_data.foo, j.object_typed_shared_data.shared_paths.foo, j.object_paths.foo FROM t_collision_compact;

-- The colliding names with different types.
CREATE TABLE t_collision_types (j JSON(SHARED foo String, object_typed_shared_data JSON(foo UInt64)))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_collision_types VALUES ('{"foo":"x","object_typed_shared_data":{"foo":100}}');
SELECT j, j.foo, j.object_typed_shared_data.foo FROM t_collision_types;

DROP TABLE t_collision;
DROP TABLE t_collision_compact;
DROP TABLE t_collision_types;
