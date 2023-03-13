SELECT
    CounterID AS k,
    quantileBFloat16(0.5)(ResolutionWidth)
FROM remote('127.0.0.{1,2}', test, hits)
GROUP BY k
ORDER BY
    count() DESC,
    CounterID ASC
LIMIT 10
SETTINGS group_by_use_nulls = 1;
