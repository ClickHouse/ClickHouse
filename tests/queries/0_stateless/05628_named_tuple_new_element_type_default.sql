-- { echo }
-- An element added to a named Tuple gets the default of its type in existing rows:
-- the first value for an Enum, not 0.

SET mutations_sync = 2;
SET allow_metadata_only_named_tuple_alter = 0;

SELECT CAST(tuple(5)::Tuple(a UInt8) AS Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2)));
SELECT CAST(tuple(5)::Tuple(a UInt8) AS Tuple(a UInt8, status Enum8('m' = -3, 'n' = 0, 'o' = 4)));
SELECT CAST(tuple(5)::Tuple(a UInt8) AS Tuple(a UInt8, t Tuple(e Enum16('x' = 1000, 'y' = 2000), s String)));
SELECT CAST(tuple(5, tuple(6))::Tuple(a UInt8, t Tuple(b UInt8)) AS Tuple(a UInt8, t Tuple(b UInt8, e Enum8('x' = 1))));
SELECT accurateCastOrNull(tuple(5)::Tuple(a UInt8), 'Tuple(a UInt8, status Enum8(\'x\' = 1, \'y\' = 2))');
SELECT CAST(tuple(5)::Tuple(a UInt8) AS Tuple(a UInt8, j JSON(e Enum8('x' = 1, 'y' = 2))));
SELECT CAST(tuple(5)::Tuple(a UInt8) AS Tuple(a UInt8, b Nullable(UInt8), c LowCardinality(String), d Array(UInt8), e Map(String, UInt8), v Variant(UInt8, String), s String));

-- ALTER ... MODIFY COLUMN as a mutation.
CREATE TABLE t_mutation (id UInt32, attrs Tuple(a UInt8), items Array(Tuple(a UInt8)), m Map(String, Tuple(a UInt8))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mutation VALUES (1, (5), [(6), (7)], {'k': (8)}), (2, (9), [], {});
ALTER TABLE t_mutation
    MODIFY COLUMN attrs Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2)),
    MODIFY COLUMN items Array(Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2))),
    MODIFY COLUMN m Map(String, Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2)));
SELECT * FROM t_mutation ORDER BY id;
OPTIMIZE TABLE t_mutation FINAL;
SELECT * FROM t_mutation ORDER BY id;

-- Metadata-only ALTER: old parts are converted on read and on merge.
CREATE TABLE t_metadata_only (id UInt32, attrs Tuple(a UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_metadata_only VALUES (1, (5));
SET allow_metadata_only_named_tuple_alter = 1;
ALTER TABLE t_metadata_only MODIFY COLUMN attrs Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2));
SET allow_metadata_only_named_tuple_alter = 0;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_metadata_only';
SELECT id, attrs, attrs.status FROM t_metadata_only;
SELECT count() FROM t_metadata_only WHERE attrs.status = 'x';
INSERT INTO t_metadata_only VALUES (2, (6, 'y'));
OPTIMIZE TABLE t_metadata_only FINAL;
SELECT * FROM t_metadata_only ORDER BY id;

-- INSERT ... SELECT from the narrower Tuple.
CREATE TABLE t_old (id UInt32, attrs Tuple(a UInt8)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_new (id UInt32, attrs Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_old VALUES (1, (5));
INSERT INTO t_new SELECT * FROM t_old;
SELECT * FROM t_new;

-- Memory converts on read.
CREATE TABLE t_memory (id UInt32, attrs Tuple(a UInt8)) ENGINE = Memory;
INSERT INTO t_memory VALUES (1, (5));
ALTER TABLE t_memory MODIFY COLUMN attrs Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2));
SELECT * FROM t_memory;

DROP TABLE t_mutation;
DROP TABLE t_metadata_only;
DROP TABLE t_old;
DROP TABLE t_new;
DROP TABLE t_memory;
