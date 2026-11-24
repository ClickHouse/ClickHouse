-- Any nonzero byte of the null map means NULL, not only 1.

-- Both rows of the column `a` have the null map byte 0x02, so the aggregate functions get no values and return NULL.
SELECT sum(a), avg(a), min(a), max(a), count(a)
FROM format(Native, 'a Nullable(UInt8), b UInt8',
    unhex('020201610f4e756c6c61626c652855496e74382902020b1601620555496e74380505'))
SETTINGS compile_aggregate_expressions = 0;

-- The rows of the column `a` have the null map bytes 0x01 and 0x02, and the nested values under NULL are different (11 and 22).
-- A key with a `String` column is serialized, and both rows are the same key (NULL, 'x').
SELECT a, b, count()
FROM format(Native, 'a Nullable(UInt8), b String',
    unhex('020201610f4e756c6c61626c652855496e74382901020b16016206537472696e6701780178'))
GROUP BY a, b
ORDER BY ALL;

SELECT a, b, count()
FROM format(Native, 'a Nullable(UInt8), b String',
    unhex('020201610f4e756c6c61626c652855496e74382901020b16016206537472696e6701780178'))
GROUP BY a, b
ORDER BY ALL
SETTINGS max_threads = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;
