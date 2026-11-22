-- `String` columns written with `string_serialization_version = 'with_dictionaries'` read back equal to a
-- `with_size_stream` twin after inserts, a merge, a mutation and a rename: a column whose granules get dictionaries, one
-- whose granules never do (unique values) and a mostly empty one. A column without dictionary granules keeps only the
-- files of `with_size_stream`, also after the rename.

DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_dict;

CREATE TABLE t_plain (id UInt64, repetitive String, unique_value String, mostly_empty String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', replace_long_file_name_to_hash = 0,
    string_serialization_version = 'with_size_stream';

CREATE TABLE t_dict (id UInt64, repetitive String, unique_value String, mostly_empty String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', replace_long_file_name_to_hash = 0,
    string_serialization_version = 'with_dictionaries';

INSERT INTO t_plain SELECT number, 'value_' || toString(number % 5), toString(cityHash64(number)), if(number % 50 = 0, 'x' || toString(number), '') FROM numbers(500);
INSERT INTO t_plain SELECT number, 'value_' || toString(number % 5), toString(cityHash64(number)), if(number % 50 = 0, 'x' || toString(number), '') FROM numbers(500, 500);
INSERT INTO t_dict SELECT number, 'value_' || toString(number % 5), toString(cityHash64(number)), if(number % 50 = 0, 'x' || toString(number), '') FROM numbers(500);
INSERT INTO t_dict SELECT number, 'value_' || toString(number % 5), toString(cityHash64(number)), if(number % 50 = 0, 'x' || toString(number), '') FROM numbers(500, 500);

SELECT 'insert', (SELECT sum(cityHash64(id, repetitive, unique_value, mostly_empty)) FROM t_plain) = (SELECT sum(cityHash64(id, repetitive, unique_value, mostly_empty)) FROM t_dict);
SELECT id, repetitive, length(unique_value), mostly_empty FROM t_dict WHERE id IN (0, 1, 499, 500, 999) ORDER BY id;
SELECT column, arrayFilter(name -> name != '', filenames) AS files FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_dict' AND active AND column IN ('repetitive', 'unique_value', 'mostly_empty') GROUP BY column, files ORDER BY column;

OPTIMIZE TABLE t_plain FINAL;
OPTIMIZE TABLE t_dict FINAL;
SELECT 'merge', (SELECT sum(cityHash64(id, repetitive, unique_value, mostly_empty)) FROM t_plain) = (SELECT sum(cityHash64(id, repetitive, unique_value, mostly_empty)) FROM t_dict);

ALTER TABLE t_plain UPDATE repetitive = repetitive || '_updated' WHERE id % 3 = 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_dict UPDATE repetitive = repetitive || '_updated' WHERE id % 3 = 0 SETTINGS mutations_sync = 2;
SELECT 'mutation', (SELECT sum(cityHash64(id, repetitive, unique_value, mostly_empty)) FROM t_plain) = (SELECT sum(cityHash64(id, repetitive, unique_value, mostly_empty)) FROM t_dict);

ALTER TABLE t_plain RENAME COLUMN unique_value TO renamed_value;
ALTER TABLE t_dict RENAME COLUMN unique_value TO renamed_value;
SELECT 'rename', (SELECT sum(cityHash64(id, repetitive, renamed_value, mostly_empty)) FROM t_plain) = (SELECT sum(cityHash64(id, repetitive, renamed_value, mostly_empty)) FROM t_dict);
SELECT column, arrayFilter(name -> name != '', filenames) AS files FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_dict' AND active AND column IN ('repetitive', 'renamed_value', 'mostly_empty') GROUP BY column, files ORDER BY column;

DROP TABLE t_plain;
DROP TABLE t_dict;
