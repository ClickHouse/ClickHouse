-- A Tuple holding JSON, filled with the default of its type, holds the default of each Enum typed path, not 0.
DROP TABLE IF EXISTS t_tuple_json_default;
DROP TABLE IF EXISTS t_tuple_json_default_skip_empty;

CREATE TABLE t_tuple_json_default
(
    id UInt32,
    t Tuple(j JSON(a Enum8('x' = 1, 'y' = 2), z Enum8('m' = -3, 'n' = 0)), d Date32, e Enum8('p' = 5, 'q' = 6), v Nullable(Enum8('p' = 5, 'q' = 6))),
    n Tuple(o Tuple(j JSON(a Enum8('x' = 1, 'y' = 2))))
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_tuple_json_default (id) VALUES (1);
SELECT 'omitted', id, t, n, toInt8(t.j.a), toInt8(t.j.z) FROM t_tuple_json_default;
-- The value hidden under a NULL element stays the column zero (insert deduplication hashes it).
SELECT 'hidden under NULL', id, toInt8(assumeNotNull(t.v)) FROM t_tuple_json_default;
ALTER TABLE t_tuple_json_default ADD COLUMN added Tuple(j JSON(a Enum8('x' = 1, 'y' = 2)));
SELECT 'add column', id, added, added.j.a FROM t_tuple_json_default;
SELECT 'default', defaultValueOfArgumentType(t), defaultValueOfTypeName('Tuple(j JSON(a Enum8(\'x\' = 1, \'y\' = 2)))') FROM t_tuple_json_default;

-- The column zero 'zero' is not the Enum default, so it is stored and reads back as 'zero'; an omitted column holds 'neg', the Enum default.
CREATE TABLE t_tuple_json_default_skip_empty (id UInt32, t Tuple(j JSON(z Enum8('neg' = -1, 'zero' = 0, 'pos' = 1))))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
SYSTEM STOP MERGES t_tuple_json_default_skip_empty;
INSERT INTO t_tuple_json_default_skip_empty VALUES (1, tuple('{"z":"zero"}'));
INSERT INTO t_tuple_json_default_skip_empty (id) VALUES (2);
SELECT 'skip empty parts with t', count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tuple_json_default_skip_empty' AND active AND column = 't';
SELECT 'skip empty', id, t FROM t_tuple_json_default_skip_empty ORDER BY id;

DROP TABLE t_tuple_json_default;
DROP TABLE t_tuple_json_default_skip_empty;
