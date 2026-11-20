-- Corrupted HyperLogLog states of `uniqCombined`, `uniqCombined64` and `uniqHLL12` throw INCORRECT_DATA instead of
-- writing out of bounds or converting an out-of-range estimate, and an estimate above UInt64 saturates.

-- `corrupted` is a large state of 100000 values with every register byte set to 0xFF: every register becomes 31 (5-bit)
-- or 63 (6-bit), above the maximum ranks 16, 48 and 21. Its rank counts are kept, so it is read as it is (the first
-- query of each pair), and a merge that raises a register to it throws.

WITH
    (SELECT unhex(hex(uniqCombinedState(number))) FROM numbers(100000)) AS legit,
    concat(substring(legit, 1, 1), repeat(unhex('FF'), 81920), substring(legit, 81922)) AS corrupted
SELECT hex(CAST(corrupted, 'AggregateFunction(uniqCombined, UInt64)')) = hex(corrupted);

WITH
    (SELECT unhex(hex(uniqCombinedState(number))) FROM numbers(100000)) AS legit,
    concat(substring(legit, 1, 1), repeat(unhex('FF'), 81920), substring(legit, 81922)) AS corrupted
SELECT length(hex(arrayReduce('uniqCombinedMergeState', [CAST(legit, 'AggregateFunction(uniqCombined, UInt64)'), CAST(corrupted, 'AggregateFunction(uniqCombined, UInt64)')]))); -- { serverError INCORRECT_DATA }

WITH
    (SELECT unhex(hex(uniqCombined64State(number))) FROM numbers(100000)) AS legit,
    concat(substring(legit, 1, 1), repeat(unhex('FF'), 98304), substring(legit, 98306)) AS corrupted
SELECT hex(CAST(corrupted, 'AggregateFunction(uniqCombined64, UInt64)')) = hex(corrupted);

WITH
    (SELECT unhex(hex(uniqCombined64State(number))) FROM numbers(100000)) AS legit,
    concat(substring(legit, 1, 1), repeat(unhex('FF'), 98304), substring(legit, 98306)) AS corrupted
SELECT length(hex(arrayReduce('uniqCombined64MergeState', [CAST(legit, 'AggregateFunction(uniqCombined64, UInt64)'), CAST(corrupted, 'AggregateFunction(uniqCombined64, UInt64)')]))); -- { serverError INCORRECT_DATA }

WITH
    (SELECT unhex(hex(uniqHLL12State(number))) FROM numbers(100000)) AS legit,
    concat(substring(legit, 1, 1), repeat(unhex('FF'), 2560), substring(legit, 2562)) AS corrupted
SELECT hex(CAST(corrupted, 'AggregateFunction(uniqHLL12, UInt64)')) = hex(corrupted);

WITH
    (SELECT unhex(hex(uniqHLL12State(number))) FROM numbers(100000)) AS legit,
    concat(substring(legit, 1, 1), repeat(unhex('FF'), 2560), substring(legit, 2562)) AS corrupted
SELECT length(hex(arrayReduce('uniqHLL12MergeState', [CAST(legit, 'AggregateFunction(uniqHLL12, UInt64)'), CAST(corrupted, 'AggregateFunction(uniqHLL12, UInt64)')]))); -- { serverError INCORRECT_DATA }

-- Rank counts that do not add up to the number of buckets (all 0).
SELECT finalizeAggregation(CAST(unhex(concat('03', repeat('00', 81920), repeat('00', 72))), 'AggregateFunction(uniqCombined, UInt64)')); -- { serverError INCORRECT_DATA }

-- A number of zero registers (the last 4 bytes, set to 1) that differs from the count of rank 0.
WITH (SELECT unhex(hex(uniqCombinedState(number))) FROM numbers(100000)) AS legit
SELECT finalizeAggregation(CAST(concat(substring(legit, 1, 81989), unhex('01000000')), 'AggregateFunction(uniqCombined, UInt64)')); -- { serverError INCORRECT_DATA }

-- These values are the preimages of 0..131071 under `intHash64`, the hash of `uniqCombined64` for UInt64, so every
-- register gets the maximum rank 48, and the estimate (about 2.66e19) is above the range of UInt64.
SELECT uniqCombined64(x) FROM
(
    WITH
        bitXor(number, bitShiftRight(number, 33)) * 0x9cb4b2f8129337db AS a,
        bitXor(a, bitShiftRight(a, 33)) * 0x4f74430c22a54005 AS b
    SELECT bitXor(b, bitShiftRight(b, 33)) AS x FROM numbers(131072)
);

-- The counts of `crafted` say that every register has rank 1, but its registers are 0. Inserting the 1000 values of
-- `small` (a hash set, not a HyperLogLog yet) into it takes the count of rank 0 below zero, which makes the estimate negative.
WITH
    (SELECT uniqCombinedState(number) FROM numbers(1000)) AS small,
    CAST(unhex(concat('03', repeat('00', 81920), '00000000', '00000200', repeat('00000000', 16))), 'AggregateFunction(uniqCombined, UInt64)') AS crafted
SELECT arrayReduce('uniqCombinedMerge', [crafted, small]); -- { serverError INCORRECT_DATA }

-- Merging `legit` raises so many registers that the counts are recounted from the registers, so the wrong counts are replaced.
WITH
    (SELECT uniqCombinedState(number) FROM numbers(100000)) AS legit,
    CAST(unhex(concat('03', repeat('00', 81920), '00000000', '00000200', repeat('00000000', 16))), 'AggregateFunction(uniqCombined, UInt64)') AS crafted
SELECT arrayReduce('uniqCombinedMerge', [crafted, legit]) = finalizeAggregation(legit);
