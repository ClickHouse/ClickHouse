-- SHARED typed paths and SHARED REGEXP rules in one JSON type: the type name and the parser, the binary type
-- encoding, the least supertype, conversions and storage in MergeTree.

SELECT '-- type name and parser';
SELECT toTypeName(CAST('{}', 'JSON(SHARED REGEXP \'^z\', SHARED b String, a UInt32, SHARED REGEXP \'^c\')'));
-- A SHARED typed path may be named REGEXP: only a string literal after SHARED REGEXP makes a rule.
SELECT toTypeName(j), j.`REGEXP` FROM (SELECT CAST('{"REGEXP":"r"}', 'JSON(SHARED REGEXP String, SHARED REGEXP \'^c\')') AS j);
SELECT formatQuery('CREATE TABLE f (j JSON(SHARED REGEXP String, SHARED a UInt32, SHARED REGEXP \'^c\')) ENGINE = Memory');

SELECT '-- binary type encoding';
-- The version byte follows the type code 0x30: 0 without both, 1 with SHARED paths only, 2 with SHARED REGEXP rules.
SELECT
    substring(hex(formatRow('RowBinary', CAST(CAST('{}', 'JSON(a UInt32)'), 'Dynamic'))), 1, 4),
    substring(hex(formatRow('RowBinary', CAST(CAST('{}', 'JSON(SHARED a UInt32)'), 'Dynamic'))), 1, 4),
    substring(hex(formatRow('RowBinary', CAST(CAST('{}', 'JSON(a UInt32, SHARED REGEXP \'^c\')'), 'Dynamic'))), 1, 4),
    substring(hex(formatRow('RowBinary', CAST(CAST('{}', 'JSON(SHARED a UInt32, SHARED REGEXP \'^c\')'), 'Dynamic'))), 1, 4);
-- Version 2 keeps the flags byte of every typed path and writes the rules after the path regexps to skip.
SELECT substring(hex(formatRow('RowBinary', CAST(CAST('{}', 'JSON(SHARED a UInt32, SHARED REGEXP \'^c\')'), 'Dynamic'))), 1, 32);
SELECT dynamicType(d), d.`JSON(a UInt32, SHARED b String, SHARED REGEXP '^c')`.b
FROM format(RowBinary, 'd Dynamic', formatRow('RowBinary', CAST(CAST('{"a":1,"b":"x","c":2}', 'JSON(a UInt32, SHARED b String, SHARED REGEXP \'^c\')'), 'Dynamic')));

SELECT '-- least supertype';
SELECT toTypeName([
    CAST('{}', 'JSON(SHARED a UInt32, SHARED b UInt32, SHARED REGEXP \'^x\', SHARED REGEXP \'^y\')'),
    CAST('{}', 'JSON(SHARED a UInt32, b UInt32, SHARED REGEXP \'^y\')')]);

SELECT '-- conversions';
-- Only the SHARED marks differ.
SELECT j.a, j.c, JSONSharedDataPaths(j)
FROM (SELECT CAST(CAST('{"a":1,"c":2}', 'JSON(SHARED a UInt32, SHARED REGEXP \'^c\')'), 'JSON(a UInt32, SHARED REGEXP \'^c\')') AS j);
-- Only the SHARED REGEXP rules differ.
SELECT toTypeName(j), j.a, j.c
FROM (SELECT CAST(CAST('{"a":1,"c":2}', 'JSON(SHARED a UInt32)'), 'JSON(SHARED a UInt32, SHARED REGEXP \'^c\')') AS j);
-- Both differ.
SELECT j.a, j.c, JSONSharedDataPaths(j)
FROM (SELECT CAST(CAST('{"a":1,"c":2}', 'JSON(SHARED a UInt32)'), 'JSON(a UInt32, SHARED REGEXP \'^c\')') AS j);

SELECT '-- MergeTree';
DROP TABLE IF EXISTS t_shared_paths_and_rules;
CREATE TABLE t_shared_paths_and_rules (x UInt64, j JSON(SHARED a UInt32, b UInt32, SHARED REGEXP '^c')) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_shared_paths_and_rules SELECT number, concat('{"a":', toString(number), ',"b":', toString(number * 2), ',"c":', toString(number * 3), ',"d":', toString(number * 4), '}') FROM numbers(4);
SELECT x, j.a, j.b, j.c, j.d, has(JSONSharedDataPaths(j), 'c') FROM t_shared_paths_and_rules ORDER BY x;
-- Both the SHARED marks and the rules change.
ALTER TABLE t_shared_paths_and_rules MODIFY COLUMN j JSON(a UInt32, SHARED b UInt32, SHARED REGEXP '^d');
SELECT x, j.a, j.b, j.c, j.d, has(JSONSharedDataPaths(j), 'd') FROM t_shared_paths_and_rules ORDER BY x;
-- Only the rules change.
ALTER TABLE t_shared_paths_and_rules MODIFY COLUMN j JSON(a UInt32, SHARED b UInt32, SHARED REGEXP '^c');
SELECT x, j.a, j.b, j.c, j.d FROM t_shared_paths_and_rules ORDER BY x;
OPTIMIZE TABLE t_shared_paths_and_rules FINAL;
SELECT x, j.a, j.b, j.c, j.d FROM t_shared_paths_and_rules ORDER BY x;
DROP TABLE t_shared_paths_and_rules;
