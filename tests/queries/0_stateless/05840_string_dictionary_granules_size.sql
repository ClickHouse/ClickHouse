-- The `.size` subcolumn of a `String` column with dictionary granules: read alone, from the codes and the lengths of the
-- dictionaries, and together with the column in both orders, with and without a filter. The rows from 900 on are unique,
-- so the last granules stay plain.

DROP TABLE IF EXISTS t_size;

CREATE TABLE t_size (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types',
    string_serialization_version = 'with_dictionaries';

INSERT INTO t_size SELECT number, multiIf(number >= 900, toString(cityHash64(number)), number % 3 = 0, '', repeat('a', number % 7)) FROM numbers(1000);

SELECT sum(length(s)), countIf(empty(s)), countIf(s = ''), countIf(notEmpty(s)) FROM t_size;
SELECT sum(s.size) FROM t_size WHERE id % 10 = 1;
SELECT id, s.size, s FROM t_size WHERE id IN (0, 1, 5, 64, 899, 900) ORDER BY id;
SELECT id, s, s.size FROM t_size WHERE id IN (0, 1, 5, 64, 899, 900) ORDER BY id;

DROP TABLE t_size;
