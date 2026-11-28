-- A serialized state of `mergedJSONPatch` has its paths sorted and without duplicates.
-- A repeated path must be rejected instead of inserting several values of the path into one row of `JSON`.

-- The state: the number of entries, then the path, the value and the sort key (`UInt64`) of every entry.
-- `a` is a typed path (`Nullable(Int64)`), `b` is a dynamic path (Dynamic binary: type `Int64` and the number).
WITH
    '01' || hex('a') || '09' || '000100000000000000' || '0100000000000000' AS entry_a,
    '01' || hex('b') || '09' || '0A0200000000000000' || '0100000000000000' AS entry_b
SELECT finalizeAggregation(CAST(unhex('02' || entry_a || entry_b), 'AggregateFunction(mergedJSONPatch, JSON(a Nullable(Int64)), UInt64)'));

WITH
    '01' || hex('b') || '09' || '0A0200000000000000' || '0100000000000000' AS entry_b
SELECT finalizeAggregation(CAST(unhex('02' || entry_b || entry_b), 'AggregateFunction(mergedJSONPatch, JSON(a Nullable(Int64)), UInt64)')); -- { serverError INCORRECT_DATA }

WITH
    '01' || hex('a') || '09' || '000100000000000000' || '0100000000000000' AS entry_a
SELECT finalizeAggregation(CAST(unhex('02' || entry_a || entry_a), 'AggregateFunction(mergedJSONPatch, JSON(a Nullable(Int64)), UInt64)')); -- { serverError INCORRECT_DATA }

WITH
    '01' || hex('a') || '09' || '000100000000000000' || '0100000000000000' AS entry_a,
    '01' || hex('b') || '09' || '0A0200000000000000' || '0100000000000000' AS entry_b
SELECT finalizeAggregation(CAST(unhex('02' || entry_b || entry_a), 'AggregateFunction(mergedJSONPatch, JSON(a Nullable(Int64)), UInt64)')); -- { serverError INCORRECT_DATA }

-- A huge number of entries must not be trusted for a preallocation.
SELECT finalizeAggregation(CAST(unhex('FFFFFFFFFFFFFFFF7F'), 'AggregateFunction(mergedJSONPatch, JSON(a Nullable(Int64)), UInt64)')); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

-- A state produced by the function itself is accepted.
SELECT finalizeAggregation(mergedJSONPatchState('{"a" : 1, "c" : {"d" : 2}, "b" : 3}'::JSON(a Nullable(Int64)), 1::UInt64));
