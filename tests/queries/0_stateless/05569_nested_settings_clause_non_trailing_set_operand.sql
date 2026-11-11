-- A `SETTINGS` clause that does not end the query applies to the operand it ends, also after the rewrites
-- of set operations move that operand to the right edge of a new node: the `UNION DISTINCT` prefix of a union,
-- the left side of `EXCEPT`, or an `EXCEPT` with a nested `INTERSECT`.

SET transform_null_in = 0;

-- The clause ends an `INTERSECT` that ends the `UNION DISTINCT` prefix: it applies to the `INTERSECT` alone.
SELECT arraySort(groupArray(x)) FROM
(
    SELECT toUInt8(getSetting('transform_null_in')) AS x
    UNION DISTINCT
    SELECT 0 INTERSECT ALL SELECT 1 SETTINGS transform_null_in = 1
    UNION ALL
    SELECT 0
);

-- The same, with `EXCEPT` nesting the `INTERSECT`.
SELECT arraySort(groupArray(x)) FROM
(
    SELECT toUInt8(getSetting('transform_null_in')) AS x
    EXCEPT ALL
    SELECT 0 INTERSECT ALL SELECT 1 SETTINGS transform_null_in = 1
    UNION ALL
    SELECT 0
);

-- The same, with `EXCEPT` nesting another `EXCEPT`.
SELECT arraySort(groupArray(x)) FROM
(
    SELECT toUInt8(getSetting('transform_null_in')) AS x
    EXCEPT ALL
    SELECT 2
    EXCEPT ALL
    SELECT 0 INTERSECT ALL SELECT 1 SETTINGS transform_null_in = 1
    UNION ALL
    SELECT 0
);

-- A bare SELECT ending the `UNION DISTINCT` prefix keeps its clause to itself.
SELECT arraySort(groupArray(x)) FROM
(
    SELECT toUInt8(getSetting('transform_null_in')) AS x
    UNION DISTINCT
    SELECT 2 SETTINGS transform_null_in = 1
    UNION ALL
    SELECT 0
);

-- So does a bare SELECT ending the left side of `EXCEPT`.
SELECT arraySort(groupArray(x)) FROM
(
    SELECT toUInt8(getSetting('transform_null_in')) AS x
    UNION ALL
    SELECT 2 SETTINGS transform_null_in = 1
    EXCEPT ALL
    SELECT 3
);

-- A clause ending an `INTERSECT` in the middle of a union applies to that `INTERSECT`.
SELECT arraySort(groupArray(x)) FROM
(
    SELECT toUInt8(getSetting('transform_null_in')) AS x INTERSECT ALL SELECT 1 SETTINGS transform_null_in = 1
    UNION ALL
    SELECT toUInt8(getSetting('transform_null_in'))
);

-- The trailing clause of the query still applies to all of it.
SELECT arraySort(groupArray(x)) FROM
(
    SELECT toUInt8(getSetting('transform_null_in')) AS x
    UNION DISTINCT
    SELECT 0 INTERSECT ALL SELECT 1 SETTINGS transform_null_in = 1
);

SELECT arraySort(groupArray(x)) FROM
(
    SELECT toUInt8(getSetting('transform_null_in')) AS x
    EXCEPT ALL
    SELECT 0 INTERSECT ALL SELECT 1 SETTINGS transform_null_in = 1
);

SELECT arraySort(groupArray(x)) FROM
(
    SELECT toUInt8(getSetting('transform_null_in')) AS x
    UNION DISTINCT
    SELECT 2
    UNION ALL
    SELECT 0 SETTINGS transform_null_in = 1
);
