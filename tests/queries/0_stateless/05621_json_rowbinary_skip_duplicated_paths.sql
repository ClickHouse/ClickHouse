-- A duplicated path in RowBinary `JSON` keeps the first value when type_json_skip_duplicated_paths = 1.
SET type_json_skip_duplicated_paths = 1;

SELECT x FROM format(RowBinary, 'x JSON(a UInt8)', unhex('02016105016100' || '01016109'));
SELECT x FROM format(RowBinary, 'x JSON(a String)', unhex('020161013101610132' || '0101610133'));
SELECT x FROM format(RowBinary, 'x JSON(a Variant(String, UInt8))', unhex('020161010501610101' || '0101610109'));
SELECT x FROM format(RowBinary, 'x JSON', unhex('0201611501310161150132' || '010161150133'));
SELECT x FROM format(RowBinary, 'x JSON', unhex('02016115013101611e2a021501010105' || '010161150133'));
SELECT x FROM format(RowBinary, 'x JSON(max_dynamic_paths=0)', unhex('0201611501390161150131' || '010161150133'));
SELECT x FROM format(RowBinary, 'x JSON(max_dynamic_paths=0)', unhex('020161000161150139' || '010161150133'));
SELECT x FROM format(RowBinary, 'x JSON(max_dynamic_paths=0)', unhex('03016215013301611501320162150131' || '010161150133'));

DROP TABLE IF EXISTS t_json_dup_rowbinary;
CREATE TABLE t_json_dup_rowbinary (id UInt8, x JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_dup_rowbinary SELECT * FROM format(RowBinary, 'id UInt8, x JSON', unhex('01' || '0201611501310161150132' || '02' || '010161150133'));
SELECT id, x FROM t_json_dup_rowbinary ORDER BY id;
DROP TABLE t_json_dup_rowbinary;

SET type_json_skip_duplicated_paths = 0;
SELECT x FROM format(RowBinary, 'x JSON(a UInt8)', unhex('02016105016100')); -- { serverError INCORRECT_DATA }
SELECT x FROM format(RowBinary, 'x JSON', unhex('0201611501310161150132')); -- { serverError INCORRECT_DATA }
SELECT x FROM format(RowBinary, 'x JSON(max_dynamic_paths=0)', unhex('020161000161150139')); -- { serverError INCORRECT_DATA }

-- A value of a path in shared data or of a skipped path is read like a value of a dynamic path:
-- a nested `JSON` value follows type_json_skip_duplicated_paths, and a `Variant` value can be read.
SELECT JSONAllPaths(x) FROM format(RowBinary, 'x JSON(max_dynamic_paths=0)', unhex('0101613000800820000000' || '0201621501310162150132')); -- { serverError INCORRECT_DATA }
SELECT x FROM format(RowBinary, 'x JSON(SKIP a)', unhex('0101613000800820000000' || '0201621501310162150132')); -- { serverError INCORRECT_DATA }
SELECT x FROM format(RowBinary, 'x JSON(max_dynamic_paths=0)', unhex('0101612315000131')); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM format(RowBinary, 'x JSON(max_dynamic_paths=0)', unhex('02' || '0161' || '3000800801000000' || '010162150178' || '0163' || '3000800801000000' || '0101622a0215010105')); -- { serverError BAD_ARGUMENTS }

SET type_json_skip_duplicated_paths = 1;
SELECT x FROM format(RowBinary, 'x JSON', unhex('0101613000800820000000' || '0201621501310162150132' || '010161150133'));
SELECT x FROM format(RowBinary, 'x JSON(max_dynamic_paths=0)', unhex('0101613000800820000000' || '0201621501310162150132' || '010161150133'));
SELECT x FROM format(RowBinary, 'x JSON(max_dynamic_paths=0)', unhex('0101611e2a021501010105' || '010161150133'));
SELECT x FROM format(RowBinary, 'x JSON(max_dynamic_paths=0)', unhex('02016115013101611e2a021501010105' || '010161150133'));
SELECT x FROM format(RowBinary, 'x JSON(SKIP a)', unhex('0101611e2a021501010105' || '010162150133'));

DROP TABLE IF EXISTS t_json_dup_rowbinary_shared;
CREATE TABLE t_json_dup_rowbinary_shared (id UInt8, x JSON(max_dynamic_paths=0)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_dup_rowbinary_shared SELECT * FROM format(RowBinary, 'id UInt8, x JSON(max_dynamic_paths=0)', unhex('01' || '0101613000800820000000' || '0201621501310162150132'));
INSERT INTO t_json_dup_rowbinary_shared SELECT * FROM format(RowBinary, 'id UInt8, x JSON(max_dynamic_paths=0)', unhex('02' || '010161150133'));
OPTIMIZE TABLE t_json_dup_rowbinary_shared FINAL;
SELECT id, x FROM t_json_dup_rowbinary_shared ORDER BY id;
DROP TABLE t_json_dup_rowbinary_shared;
