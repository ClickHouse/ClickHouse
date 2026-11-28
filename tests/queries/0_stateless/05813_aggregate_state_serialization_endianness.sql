-- Persisted aggregate states must have the same bytes on little and big endian hosts:
-- fixed-size numbers are little endian, `IPv6` is in network order, and the length fields are little endian.

SELECT 'serialize';
SELECT hex(groupUniqArrayState(tuple(toUInt32(16909060), toIPv6('2001:db8::102:304'), ['ab'])));
SELECT hex(groupUniqArrayState(tuple(toLowCardinality(toUInt32(16909060)), toIPv6('2001:db8::102:304'), ['ab'])));
SELECT hex(groupUniqArrayState(tuple(toUInt32(16909060), toDecimal64(1.5, 2))));

SELECT 'deserialize';
SELECT finalizeAggregation(CAST(unhex('01270403020120010DB800000000000000000102030401000000000000000300000000000000616200'), 'AggregateFunction(groupUniqArray, Tuple(UInt32, IPv6, Array(String)))'));
SELECT finalizeAggregation(CAST(unhex('01270403020120010DB800000000000000000102030401000000000000000300000000000000616200'), 'AggregateFunction(groupUniqArray, Tuple(LowCardinality(UInt32), IPv6, Array(String)))'));
SELECT finalizeAggregation(CAST(unhex('010C040302019600000000000000'), 'AggregateFunction(groupUniqArray, Tuple(UInt32, Decimal64(2)))'));
