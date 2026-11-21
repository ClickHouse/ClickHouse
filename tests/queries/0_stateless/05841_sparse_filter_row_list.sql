-- Filters by `PREWHERE` conditions on sparse columns, whose filters list the rows they keep. Every case compares the
-- result with the same data stored full.

DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_full;

-- At threshold 0 every column that holds a default value is sparse, at threshold 1 none is. A sparse `Nullable` column
-- needs the serialization info `with_types`. The reading drops the tails of granules only from a Wide part.
CREATE TABLE t_sparse (n UInt64, s String, w String, w2 String, f Nullable(UInt8)) ENGINE = MergeTree ORDER BY n
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0, nullable_serialization_version = 'allow_sparse',
    serialization_info_version = 'with_types', index_granularity = 64, min_bytes_for_wide_part = 0;
CREATE TABLE t_full (n UInt64, s String, w String, w2 String, f Nullable(UInt8)) ENGINE = MergeTree ORDER BY n
    SETTINGS ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 64, min_bytes_for_wide_part = 0;

-- `s` has values only in the rows 0, 2 and 5 of each granule of 64 rows. `w` and `w2` have values in the same rows,
-- every third one. `f` is null in two rows of three, and 0 or 1 in the others.
INSERT INTO t_sparse SELECT
    number,
    if(number % 64 IN (0, 2, 5), 'a' || toString(number), ''),
    if(number % 3 = 0, 'b' || toString(number), ''),
    if(number % 3 = 0, 'c' || toString(number), ''),
    if(number % 3 = 0, number % 2, NULL)
FROM numbers(6400);
INSERT INTO t_full SELECT * FROM t_sparse;

SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse' AND active AND column != 'n';

-- The filter keeps a third of the rows, so `PREWHERE` applies it, and the columns read after it are gathered by its
-- rows. `w2` has values in the same rows as `w`, so it keeps its values as they are.
SELECT 'gathered by the kept rows',
    (SELECT (count(), sum(cityHash64(n, w, w2))) FROM t_sparse PREWHERE w != '')
    = (SELECT (count(), sum(cityHash64(n, w, w2))) FROM t_full PREWHERE w != '');

-- The rows after the last kept row of each granule are dropped from the reading, which takes the filter as a mask.
SELECT 'granule tails dropped',
    (SELECT (count(), sum(cityHash64(n, s, w))) FROM t_sparse PREWHERE s != '')
    = (SELECT (count(), sum(cityHash64(n, s, w))) FROM t_full PREWHERE s != '');

-- Two `PREWHERE` steps, whose filters are combined as masks.
SELECT 'two steps',
    (SELECT (count(), sum(cityHash64(n, w, w2))) FROM t_sparse PREWHERE w != '' AND n % 7 = 0)
    = (SELECT (count(), sum(cityHash64(n, w, w2))) FROM t_full PREWHERE w != '' AND n % 7 = 0)
SETTINGS enable_multiple_prewhere_read_steps = 1;

-- A `Nullable(UInt8)` filter keeps its rows that are neither null nor 0.
SELECT 'Nullable filter',
    (SELECT (count(), sum(cityHash64(n, w))) FROM t_sparse PREWHERE f)
    = (SELECT (count(), sum(cityHash64(n, w))) FROM t_full PREWHERE f);

DROP TABLE t_sparse;
DROP TABLE t_full;
