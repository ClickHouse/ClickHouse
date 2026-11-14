-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- `mergeSerializedHLL` without an explicit `lg_k` must keep the precision of its inputs even when
-- they are small (sparse) sketches, instead of producing a sketch with the maximum `lg_k`.
-- Byte 4 of a serialized HLL sketch is its `lg_k`.

DROP TABLE IF EXISTS sketches;
CREATE TABLE sketches (s String) ENGINE = Memory;
INSERT INTO sketches SELECT serializedHLL(10)(number) FROM numbers(100000) GROUP BY intDiv(number, 10);

SELECT reinterpretAsUInt8(substring(m, 4, 1)), length(m) < 1000, abs(cardinalityFromHLL(m) - 100000) < 10000
FROM (SELECT mergeSerializedHLL(s) AS m FROM sketches);

-- Partial states shipped between servers.
SELECT reinterpretAsUInt8(substring(m, 4, 1)), length(m) < 1000, abs(cardinalityFromHLL(m) - 100000) < 10000
FROM (SELECT mergeSerializedHLL(s) AS m FROM remote('127.0.0.{1,2}', currentDatabase(), sketches));

-- An explicit `lg_k` still wins.
SELECT reinterpretAsUInt8(substring(mergeSerializedHLL(0, 12)(s), 4, 1)) FROM sketches;

DROP TABLE sketches;
