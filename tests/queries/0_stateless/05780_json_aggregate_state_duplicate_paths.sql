-- A serialized value of a `JSON` column in an aggregate state (here of `groupArray`) has every path of a row once,
-- in sorted order. A repeated path must be rejected instead of producing a column with subcolumns of different sizes.

-- The state of `groupArray` with one row: the number of paths, then the size of the path, the path,
-- the size of the value and the value (Dynamic binary: type `Int64` and the number) for every path.
WITH
    '0100000000000000' || hex('a') || '0900000000000000' || '0A0100000000000000' AS path_a,
    '0100000000000000' || hex('b') || '0900000000000000' || '0A0200000000000000' AS path_b,
    '013C' || '0200000000000000' AS header
SELECT
    finalizeAggregation(CAST(unhex(header || path_a || path_b), 'AggregateFunction(groupArray, JSON)')),
    finalizeAggregation(CAST(unhex(header || path_a || path_b), 'AggregateFunction(groupArray, JSON(max_dynamic_paths = 0))'));

-- The same path twice: into a dynamic path and into shared data.
WITH
    '0100000000000000' || hex('a') || '0900000000000000' || '0A0100000000000000' AS path_a,
    '013C' || '0200000000000000' AS header
SELECT finalizeAggregation(CAST(unhex(header || path_a || path_a), 'AggregateFunction(groupArray, JSON)')); -- { serverError INCORRECT_DATA }

WITH
    '0100000000000000' || hex('a') || '0900000000000000' || '0A0100000000000000' AS path_a,
    '013C' || '0200000000000000' AS header
SELECT finalizeAggregation(CAST(unhex(header || path_a || path_a), 'AggregateFunction(groupArray, JSON(max_dynamic_paths = 0))')); -- { serverError INCORRECT_DATA }

-- Paths out of order in shared data.
WITH
    '0100000000000000' || hex('a') || '0900000000000000' || '0A0100000000000000' AS path_a,
    '0100000000000000' || hex('b') || '0900000000000000' || '0A0200000000000000' AS path_b,
    '013C' || '0200000000000000' AS header
SELECT finalizeAggregation(CAST(unhex(header || path_b || path_a), 'AggregateFunction(groupArray, JSON(max_dynamic_paths = 0))')); -- { serverError INCORRECT_DATA }

-- A typed path repeated as a dynamic path or in shared data. Typed paths come first in the serialized value.
SELECT finalizeAggregation(CAST(
    unhex('012a0300000000000000010000000000000001000000000000006109000000000000000a0100000000000000'),
    'AggregateFunction(groupArray, JSON(a Int64))')); -- { serverError INCORRECT_DATA }

SELECT finalizeAggregation(CAST(
    unhex('012a0300000000000000010000000000000001000000000000006109000000000000000a0100000000000000'),
    'AggregateFunction(groupArray, JSON(a Int64, max_dynamic_paths = 0))')); -- { serverError INCORRECT_DATA }
