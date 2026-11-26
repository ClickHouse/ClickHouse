-- Tags: no-fasttest
-- no-fasttest: requires the ORC input/output format, which is not built in fasttest.

-- uniontype<struct<AB:int,ab:string>,struct<Ab:bigint>> read as Variant(Tuple(AB Int64), Tuple(Ab Int64)): `Ab` binds only
-- the second branch, as `AB` and `ab` make it ambiguous in the first, also after `ab` is pruned from the read.

SET allow_suspicious_variant_types = 1;
SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05894.orc', ORC,
    'v Variant(Tuple(AB Int32, ab String), Tuple(Ab Int64)), a Array(Variant(Tuple(AB Int32, ab String), Tuple(Ab Int64)))')
SELECT CAST(CAST((11, 'x'), 'Tuple(AB Int32, ab String)'), 'Variant(Tuple(AB Int32, ab String), Tuple(Ab Int64))') AS v, [v]
UNION ALL
SELECT CAST(CAST(tuple(22), 'Tuple(Ab Int64)'), 'Variant(Tuple(AB Int32, ab String), Tuple(Ab Int64))') AS v, [v];

SELECT v, variantType(v), a FROM file(currentDatabase() || '_05894.orc', ORC,
    'v Variant(Tuple(AB Int64), Tuple(Ab Int64)), a Array(Variant(Tuple(AB Int64), Tuple(Ab Int64)))')
ORDER BY toString(v) SETTINGS input_format_orc_case_insensitive_column_matching = 0;
SELECT v, variantType(v) FROM file(currentDatabase() || '_05894.orc', ORC, 'v Variant(Tuple(AB Int64), Tuple(Ab Int64))')
ORDER BY toString(v) SETTINGS input_format_orc_case_insensitive_column_matching = 1;
SELECT a FROM file(currentDatabase() || '_05894.orc', ORC, 'a Array(Variant(Tuple(AB Int64), Tuple(Ab Int64)))')
ORDER BY toString(a) SETTINGS input_format_orc_case_insensitive_column_matching = 1;
