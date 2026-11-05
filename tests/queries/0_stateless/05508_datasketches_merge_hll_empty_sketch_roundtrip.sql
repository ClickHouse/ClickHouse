-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- `mergeSerializedHLL` without parameters must round-trip empty Apache DataSketches HLL
-- sketches produced upstream instead of re-emitting an empty sketch with the default
-- configuration. The compact empty sketch preamble stores lg_k in byte 4 and the target
-- type in bits 2-3 of byte 8 (0 = HLL_4, 1 = HLL_6, 2 = HLL_8).

-- Empty sketch, lg_k = 14, HLL_8.
SELECT 'single empty sketch round-trips unchanged';
SELECT hex(mergeSerializedHLL(unhex('0201070E030C0008')));

SELECT 'single empty sketch round-trips unchanged through distributed aggregation';
SELECT hex(mergeSerializedHLL(unhex('0201070E030C0008')))
FROM remote('127.0.0.{1,2}', system.one);

-- Empty sketch, lg_k = 12, HLL_6, together with the one above.
SELECT 'several empty sketches merge order-independently';
SELECT
    reinterpretAsUInt8(substring(merged, 4, 1)) AS lg_k,
    bitAnd(bitShiftRight(reinterpretAsUInt8(substring(merged, 8, 1)), 2), 3) AS target_type,
    cardinalityFromHLL(merged) AS estimate
FROM
(
    SELECT mergeSerializedHLL(x) AS merged
    FROM (SELECT arrayJoin([unhex('0201070E030C0008'), unhex('0201070C030C0004')]) AS x)
    UNION ALL
    SELECT mergeSerializedHLL(x) AS merged
    FROM (SELECT arrayJoin([unhex('0201070C030C0004'), unhex('0201070E030C0008')]) AS x)
);

SELECT 'empty sketches do not affect a merge with data';
WITH (SELECT serializedHLL(14, 'HLL_8')(number) FROM numbers(100000)) AS sk
SELECT
    (SELECT hex(mergeSerializedHLL(x)) FROM (SELECT arrayJoin([unhex('0201070C030C0004'), sk]) AS x))
    = (SELECT hex(mergeSerializedHLL(x)) FROM (SELECT sk AS x));

SELECT 'explicit parameters still take precedence';
SELECT
    reinterpretAsUInt8(substring(merged, 4, 1)) AS lg_k,
    bitAnd(bitShiftRight(reinterpretAsUInt8(substring(merged, 8, 1)), 2), 3) AS target_type
FROM (SELECT mergeSerializedHLL(0, 10, 'HLL_4')(unhex('0201070E030C0008')) AS merged);
