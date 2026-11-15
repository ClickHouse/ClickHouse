-- A serialized `mergedJSONPatch` state starts with its number of entries. It is only a declaration: a state that
-- declares more entries than it contains must report the truncation, not reserve memory for the declared count.

-- The largest count a varint encodes.
SELECT CAST(unhex('ffffffffffffffffff01'), 'AggregateFunction(mergedJSONPatch, JSON(a Nullable(Int64)), UInt64)'); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

-- 99999999 entries and no payload: reserving the declared count needs gigabytes.
SELECT CAST(unhex('FFC1D72F'), 'AggregateFunction(mergedJSONPatch, JSON(a Nullable(Int64)), UInt64)') SETTINGS max_memory_usage = 100000000; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

-- A state that holds what it declares is read as before. `a` is a typed path, `b` a dynamic one.
WITH
    '01' || hex('a') || '09' || '000100000000000000' || '0100000000000000' AS entry_a,
    '01' || hex('b') || '09' || '0A0200000000000000' || '0100000000000000' AS entry_b
SELECT finalizeAggregation(CAST(unhex('02' || entry_a || entry_b), 'AggregateFunction(mergedJSONPatch, JSON(a Nullable(Int64)), UInt64)'));
