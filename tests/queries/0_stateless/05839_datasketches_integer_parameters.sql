-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- `lg_k` and the `base64_encoded` flag are discrete parameters. A fractional or negative literal
-- must be rejected instead of being truncated (`0.5` -> `0`, `10.9` -> `10`) or wrapped around.

SELECT serializedHLL(10.9)(number) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT serializedHLL(-10)(number) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT serializedHLL(10.9::Decimal(9, 1))(number) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT serializedHLL('10')(number) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT mergeSerializedHLL(0.5)(sketch) FROM (SELECT serializedHLL(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedHLL(1.5)(sketch) FROM (SELECT serializedHLL(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedHLL(10.9)(sketch) FROM (SELECT serializedHLL(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedHLL(-1)(sketch) FROM (SELECT serializedHLL(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedHLL(10.9, 'HLL_4')(sketch) FROM (SELECT serializedHLL(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedHLL(0.5, 10)(sketch) FROM (SELECT serializedHLL(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedHLL(0, 10.9)(sketch) FROM (SELECT serializedHLL(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedHLL(0, 10.9, 'HLL_4')(sketch) FROM (SELECT serializedHLL(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedHLL(10, 'HLL_4', 0.5)(sketch) FROM (SELECT serializedHLL(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT mergeSerializedQuantiles(0.5)(sketch) FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedQuantiles(1.5)(sketch) FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedQuantiles(-1)(sketch) FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT mergeSerializedTDigest(0.5)(sketch) FROM (SELECT serializedTDigest(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedTDigest(1.5)(sketch) FROM (SELECT serializedTDigest(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedTDigest(-1)(sketch) FROM (SELECT serializedTDigest(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Integer literals still work.
SELECT cardinalityFromHLL(serializedHLL(10)(number)) FROM numbers(10);
SELECT cardinalityFromHLL(mergeSerializedHLL(0)(sketch)) FROM (SELECT serializedHLL(number) AS sketch FROM numbers(10));
SELECT cardinalityFromHLL(mergeSerializedHLL(10)(sketch)) FROM (SELECT serializedHLL(number) AS sketch FROM numbers(10));
SELECT cardinalityFromHLL(mergeSerializedHLL(0, 10, 'HLL_4')(sketch)) FROM (SELECT serializedHLL(number) AS sketch FROM numbers(10));
SELECT length(mergeSerializedQuantiles(0)(sketch)) > 0 FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(10));
SELECT length(mergeSerializedTDigest(0)(sketch)) > 0 FROM (SELECT serializedTDigest(number) AS sketch FROM numbers(10));
