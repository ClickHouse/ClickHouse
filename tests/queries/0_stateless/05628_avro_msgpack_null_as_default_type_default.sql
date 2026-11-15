-- Tags: no-fasttest
-- no-fasttest: the Avro and MsgPack formats are not built in the fast test

-- An explicit null read with `input_format_null_as_default` stores the default of the column type, not the zero of the column.
DROP TABLE IF EXISTS t_null_as_default_src;
CREATE TABLE t_null_as_default_src
(
    e Nullable(Enum8('a' = 1, 'b' = 2)),
    m Nullable(Enum8('m' = -3, 'n' = 0, 'o' = 4)),
    i Nullable(Int32),
    ae Array(Nullable(Enum8('a' = 1, 'b' = 2))),
    me Map(String, Nullable(Enum8('a' = 1, 'b' = 2))),
    te Tuple(x Nullable(Enum8('a' = 1, 'b' = 2))),
    t Nullable(Tuple(x Enum8('a' = 1, 'b' = 2)))
) ENGINE = Memory;
INSERT INTO t_null_as_default_src VALUES (NULL, NULL, NULL, [NULL, 'b'], {'k': NULL}, tuple(NULL), NULL);
INSERT INTO FUNCTION file(currentDatabase() || '_null_as_default.avro') SELECT * FROM t_null_as_default_src SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_null_as_default.msgpack') SELECT * FROM t_null_as_default_src SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_null_as_default_null.avro') SELECT NULL AS n SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_null_as_default_variant.avro') SELECT CAST(NULL AS Nullable(Tuple(metadata String, value String))) AS v SETTINGS engine_file_truncate_on_insert = 1, output_format_avro_string_column_pattern = '^never$';

SELECT 'Avro', * FROM file(currentDatabase() || '_null_as_default.avro', Avro, 'e Enum8(\'a\' = 1, \'b\' = 2), m Enum8(\'m\' = -3, \'n\' = 0, \'o\' = 4), i Enum8(\'a\' = 1, \'b\' = 2), ae Array(Enum8(\'a\' = 1, \'b\' = 2)), me Map(String, Enum8(\'a\' = 1, \'b\' = 2)), te Tuple(x Enum8(\'a\' = 1, \'b\' = 2)), t Tuple(x Enum8(\'a\' = 1, \'b\' = 2))');
SELECT 'Avro null type', * FROM file(currentDatabase() || '_null_as_default_null.avro', Avro, 'n Enum8(\'a\' = 1, \'b\' = 2)');
SELECT 'Avro Nothing', toTypeName(n), count() FROM file(currentDatabase() || '_null_as_default_null.avro', Avro, 'n Nothing') GROUP BY 2;
SELECT 'Avro JSON', * FROM file(currentDatabase() || '_null_as_default_variant.avro', Avro, 'v JSON(a Enum8(\'x\' = 5))');
SELECT 'MsgPack', * FROM file(currentDatabase() || '_null_as_default.msgpack', MsgPack, 'e Enum8(\'a\' = 1, \'b\' = 2), m Enum8(\'m\' = -3, \'n\' = 0, \'o\' = 4), i Enum8(\'a\' = 1, \'b\' = 2), ae Array(Enum8(\'a\' = 1, \'b\' = 2)), me Map(String, Enum8(\'a\' = 1, \'b\' = 2)), te Tuple(x Enum8(\'a\' = 1, \'b\' = 2)), t Tuple(x Enum8(\'a\' = 1, \'b\' = 2))');
SELECT 'MsgPack JSON', * FROM format(MsgPack, 'j JSON(a Enum8(\'x\' = 5))', x'c0');
SELECT 'MsgPack Nothing', toTypeName(x), count() FROM format(MsgPack, 'x Nothing', x'c0') GROUP BY 2;

DROP TABLE IF EXISTS t_null_as_default_dst;
CREATE TABLE t_null_as_default_dst (e Enum8('a' = 1, 'b' = 2), m Enum8('m' = -3, 'n' = 0, 'o' = 4) DEFAULT 'o') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null_as_default_dst SELECT e, m FROM file(currentDatabase() || '_null_as_default.avro', Avro, 'e Enum8(\'a\' = 1, \'b\' = 2), m Enum8(\'m\' = -3, \'n\' = 0, \'o\' = 4)');
SELECT 'MergeTree', * FROM t_null_as_default_dst;
TRUNCATE TABLE t_null_as_default_dst;
INSERT INTO t_null_as_default_dst SELECT * FROM file(currentDatabase() || '_null_as_default.avro');
SELECT 'MergeTree with DEFAULT', * FROM t_null_as_default_dst;
DROP TABLE t_null_as_default_dst;
DROP TABLE t_null_as_default_src;
