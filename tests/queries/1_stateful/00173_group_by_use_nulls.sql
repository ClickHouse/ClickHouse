-- Tags: no-parallel-replicas
-- It uses remote over a table with parallel replicas
-- And 127.0.0.2 threated as remote address, but RemoteSource for it connects to the scheduler
-- This has to be fixed.

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
