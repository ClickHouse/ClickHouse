-- Rounding functions with a non-constant scale must not throw on rows that a condition skips.

SET short_circuit_function_evaluation = 'enable';

SELECT s, if(s < 100, round(1.55, s), 0) FROM (SELECT arrayJoin([toUInt64(1), toUInt64(18446744073709551614)]) AS s) ORDER BY s;
SELECT s, if(s < 100, floor(1.55, s), 0) FROM (SELECT arrayJoin([toUInt32(1), toUInt32(40000)]) AS s) ORDER BY s;
SELECT s, if(s < 100, ceil(1.55, s), 0) FROM (SELECT arrayJoin([toUInt32(1), toUInt32(40000)]) AS s) ORDER BY s;
SELECT s, if(s < 100, trunc(1.55, s), 0) FROM (SELECT arrayJoin([toUInt32(1), toUInt32(40000)]) AS s) ORDER BY s;
SELECT s, if(s < 100, roundBankers(1.55, s), 0) FROM (SELECT arrayJoin([toUInt32(1), toUInt32(40000)]) AS s) ORDER BY s;
SELECT s, if(s > -100, round(1234, s), 0) FROM (SELECT arrayJoin([toInt32(-2), toInt32(-40000)]) AS s) ORDER BY s;

SELECT s, multiIf(s >= 100, 0, floor(toDecimal32(1.25, 2), s)) FROM (SELECT arrayJoin([toUInt16(1), toUInt16(40000)]) AS s) ORDER BY s;
SELECT s, multiIf(s >= 100, 0, floor(toDecimal64(1.25, 2), s)) FROM (SELECT arrayJoin([toUInt16(1), toUInt16(40000)]) AS s) ORDER BY s;
SELECT s, multiIf(s >= 100, 0, floor(toDecimal128(1.25, 2), s)) FROM (SELECT arrayJoin([toUInt16(1), toUInt16(40000)]) AS s) ORDER BY s;
SELECT s, multiIf(s >= 100, 0, floor(toDecimal256(1.25, 2), s)) FROM (SELECT arrayJoin([toUInt16(1), toUInt16(40000)]) AS s) ORDER BY s;

SELECT s, s < 100 AND round(1.55, s) > 1 FROM (SELECT arrayJoin([toUInt64(1), toUInt64(18446744073709551614)]) AS s) ORDER BY s;

-- Unused LowCardinality dictionary entries and values under NULL are not evaluated either.
SELECT s, if(s < 100, round(1.55, s), 0) FROM (SELECT toLowCardinality(arrayJoin([toUInt32(1), toUInt32(40000)])) AS s) ORDER BY s;
SELECT s, if(s > -100, round(1234, s), 0) FROM (SELECT toLowCardinality(arrayJoin([toInt32(-2), toInt32(-40000)])) AS s) ORDER BY s;
SELECT s, round(1.55, nullIf(s, 40000)) FROM (SELECT arrayJoin([toUInt32(1), toUInt32(40000)]) AS s) ORDER BY s;

-- Rows that are evaluated still throw.
SELECT round(1.55, s) FROM (SELECT arrayJoin([toUInt64(1), toUInt64(18446744073709551614)]) AS s); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT if(s > 0, round(1.55, s), 0) FROM (SELECT arrayJoin([toUInt64(1), toUInt64(18446744073709551614)]) AS s); -- { serverError ARGUMENT_OUT_OF_BOUND }
