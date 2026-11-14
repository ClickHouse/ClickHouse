-- A duplicated path in RowBinary `JSON` keeps the first value when type_json_skip_duplicated_paths = 1.
SET type_json_skip_duplicated_paths = 1;

SELECT x FROM format(RowBinary, 'x JSON(a UInt8)', unhex('02016105016100' || '01016109'));
SELECT x FROM format(RowBinary, 'x JSON(a String)', unhex('020161013101610132' || '0101610133'));
SELECT x FROM format(RowBinary, 'x JSON(a Variant(String, UInt8))', unhex('02016101050161010101' || '0101610109'));
SELECT x FROM format(RowBinary, 'x JSON', unhex('0201611501310161150132' || '010161150133'));

DROP TABLE IF EXISTS t_json_dup_rowbinary;
CREATE TABLE t_json_dup_rowbinary (id UInt8, x JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_dup_rowbinary SELECT * FROM format(RowBinary, 'id UInt8, x JSON', unhex('01' || '0201611501310161150132' || '02' || '010161150133'));
SELECT id, x FROM t_json_dup_rowbinary ORDER BY id;
DROP TABLE t_json_dup_rowbinary;

SET type_json_skip_duplicated_paths = 0;
SELECT x FROM format(RowBinary, 'x JSON(a UInt8)', unhex('02016105016100')); -- { serverError INCORRECT_DATA }
SELECT x FROM format(RowBinary, 'x JSON', unhex('0201611501310161150132')); -- { serverError INCORRECT_DATA }
