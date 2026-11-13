-- 1. Searching a constant array for a UUID gives the same answer as searching the materialized array
--    when either side is a `Dynamic` holding a `UUID2` or `UUID` value.
-- 2. A lambda declared inside an `APPLY` expression and shadowing a string function is not folded
--    inside a type name argument by `uuid_type_version = 2`.

SELECT '-- has';
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT
    has([CAST(toUUID2(s) AS Dynamic)], toUUID2(s)),
    has(materialize([CAST(toUUID2(s) AS Dynamic)]), toUUID2(s)),
    has([CAST(toUUID2(s) AS Dynamic)], toUUID(s)),
    has(materialize([CAST(toUUID2(s) AS Dynamic)]), toUUID(s)),
    has([toUUID2(s)], CAST(toUUID2(s) AS Dynamic)),
    has(materialize([toUUID2(s)]), CAST(toUUID2(s) AS Dynamic));

SELECT '-- indexOf, countEqual';
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT
    indexOf([CAST(toUUID2(s) AS Dynamic)], toUUID2(s)),
    indexOf(materialize([CAST(toUUID2(s) AS Dynamic)]), toUUID2(s)),
    countEqual([CAST(toUUID2(s) AS Dynamic)], toUUID2(s)),
    countEqual(materialize([CAST(toUUID2(s) AS Dynamic)]), toUUID2(s)),
    indexOf([toUUID(s)], CAST(toUUID(s) AS Dynamic)),
    indexOf(materialize([toUUID(s)]), CAST(toUUID(s) AS Dynamic));

SELECT '-- apply lambda';
SET uuid_type_version = 2;
DROP TABLE IF EXISTS t_apply_lambda;
CREATE TABLE t_apply_lambda ENGINE = Memory AS
SELECT * APPLY(x -> (
    WITH ((a, b) -> 'String') AS concat
    SELECT CAST('x', concat('U', 'UID'))))
FROM values('s String', 'x');
SELECT * FROM t_apply_lambda;
DROP TABLE t_apply_lambda;
