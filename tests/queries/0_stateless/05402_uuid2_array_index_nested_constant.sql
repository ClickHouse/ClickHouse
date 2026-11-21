-- `has`, `indexOf` and `countEqual` find a value the same way for a constant and a materialized haystack
-- when `UUID` and `UUID2` are mixed inside composite elements.
-- See https://github.com/ClickHouse/ClickHouse/pull/110084

WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT
    has([tuple(toUUID2(s))], tuple(toUUID(s))),
    has(materialize([tuple(toUUID2(s))]), tuple(toUUID(s))),
    has([tuple(toUUID(s))], tuple(toUUID2(s))),
    has(materialize([tuple(toUUID(s))]), tuple(toUUID2(s)));

WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT
    indexOf([tuple(toUUID2('00000000-0000-0000-0000-000000000002')), tuple(toUUID2(s))], tuple(toUUID(s))),
    indexOf(materialize([tuple(toUUID2('00000000-0000-0000-0000-000000000002')), tuple(toUUID2(s))]), tuple(toUUID(s))),
    countEqual([tuple(toUUID2(s)), tuple(toUUID2(s))], tuple(toUUID(s))),
    countEqual(materialize([tuple(toUUID2(s)), tuple(toUUID2(s))]), tuple(toUUID(s)));

-- Arrays as elements.
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT
    has([[toUUID2(s)]], [toUUID(s)]),
    has(materialize([[toUUID2(s)]]), [toUUID(s)]),
    has([[toUUID(s)]], materialize([toUUID2(s)])),
    has([[toUUID2(s)]], [toUUID2('00000000-0000-0000-0000-000000000002')]);

-- Nullable needle.
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT
    has([tuple(toUUID2(s))], toNullable(tuple(toUUID(s)))),
    has([tuple(toUUID2(s))], CAST(NULL, 'Nullable(Tuple(UUID))'));
