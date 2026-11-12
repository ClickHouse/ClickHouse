-- A JSON with SHARED paths as the value of a Dynamic path is stored inside the dynamic shared data of the outer JSON.
-- A whole read of that path hands the nested typed shared data one sequential buffer, so it must read its substreams
-- in write order and not seek to the marks of substreams the container cannot position.

DROP TABLE IF EXISTS t_w_advanced;
DROP TABLE IF EXISTS t_w_chunked;
DROP TABLE IF EXISTS t_c_advanced;

CREATE TABLE t_w_advanced (x UInt64, j JSON(max_dynamic_paths=0)) ENGINE = MergeTree ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, object_shared_data_serialization_version = 'advanced', object_shared_data_serialization_version_for_zero_level_parts = 'advanced';
CREATE TABLE t_w_chunked (x UInt64, j JSON(max_dynamic_paths=0)) ENGINE = MergeTree ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, object_shared_data_serialization_version = 'advanced_chunked', object_shared_data_serialization_version_for_zero_level_parts = 'advanced_chunked';
CREATE TABLE t_c_advanced (x UInt64, j JSON(max_dynamic_paths=0)) ENGINE = MergeTree ORDER BY x
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, object_shared_data_serialization_version = 'advanced', object_shared_data_serialization_version_for_zero_level_parts = 'advanced';

-- The row binary of the outer JSON: one path, `d`, whose Dynamic value has the type JSON(SHARED a UInt64).
INSERT INTO t_w_advanced SELECT 1, j FROM format(RowBinary, 'j JSON(max_dynamic_paths=0)', concat(unhex('010164'), formatRow('RowBinary', CAST(CAST('{"a":100}', 'JSON(SHARED a UInt64)'), 'Dynamic'))));
INSERT INTO t_w_advanced SELECT 2, j FROM format(RowBinary, 'j JSON(max_dynamic_paths=0)', concat(unhex('010164'), formatRow('RowBinary', CAST(CAST('{"a":101}', 'JSON(SHARED a UInt64)'), 'Dynamic'))));
INSERT INTO t_w_chunked SELECT * FROM t_w_advanced;
INSERT INTO t_c_advanced SELECT * FROM t_w_advanced;

SELECT 'two parts';
SELECT 'w advanced', x, j.d, j.d.:`JSON(SHARED a UInt64)`.a, j FROM t_w_advanced ORDER BY x;
SELECT 'w chunked', x, j.d, j.d.:`JSON(SHARED a UInt64)`.a, j FROM t_w_chunked ORDER BY x;
SELECT 'c advanced', x, j.d, j.d.:`JSON(SHARED a UInt64)`.a, j FROM t_c_advanced ORDER BY x;

OPTIMIZE TABLE t_w_advanced FINAL;
OPTIMIZE TABLE t_w_chunked FINAL;
OPTIMIZE TABLE t_c_advanced FINAL;

SELECT 'merged';
SELECT 'w advanced', x, j.d, j.d.:`JSON(SHARED a UInt64)`.a, j FROM t_w_advanced ORDER BY x;
SELECT 'w chunked', x, j.d, j.d.:`JSON(SHARED a UInt64)`.a, j FROM t_w_chunked ORDER BY x;
SELECT 'c advanced', x, j.d, j.d.:`JSON(SHARED a UInt64)`.a, j FROM t_c_advanced ORDER BY x;

DROP TABLE t_w_advanced;
DROP TABLE t_w_chunked;
DROP TABLE t_c_advanced;
