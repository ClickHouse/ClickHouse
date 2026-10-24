-- Bar levels of wide integer and wide `Decimal` results of `-Sparkbar` must be computed without
-- overflow: the product of a result near the maximum of the type and the number of levels does not
-- fit into the type. It used to wrap around and index past the end of the bar characters.

SELECT sumSparkbar(4, 0, 3)(number, toInt256(number) * toInt256('10000000000000000000000000000000000000000000000000000000000000000000000000000')) FROM numbers(4);
SELECT sumSparkbar(4, 0, 3)(number, toUInt256(number) * toUInt256('20000000000000000000000000000000000000000000000000000000000000000000000000000')) FROM numbers(4);
SELECT sumSparkbar(4, 0, 3)(number, toInt128(number) * toInt128('50000000000000000000000000000000000000')) FROM numbers(4);
SELECT sumSparkbar(4, 0, 3)(number, toUInt128(number) * toUInt128('100000000000000000000000000000000000000')) FROM numbers(4);
SELECT sumSparkbar(4, 0, 3)(number, toDecimal256(number, 0) * toDecimal256('1e75', 0)) FROM numbers(4);
SELECT maxSparkbar(2, 0, 1)(number, if(number = 0, toInt256('-1'), toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967'))) FROM numbers(2);

-- The query found by the AST fuzzer.
SELECT length(sumSparkbarDistinct(7, 2, 1024)(number, byteSwap(toInt256OrDefault(concat(number))))) > 0 FROM numbers(1024);

-- A bare `NULL` key with `DateTime64` bounds.
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT countSparkbar(3, toDateTime64('2024-01-01 00:00:00', 3, 'UTC'), toDateTime64('2024-01-03 00:00:00', 3, 'UTC'))(NULL) AS r FROM numbers(3));
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbar(3, toDateTime64('2024-01-01 00:00:00', 3, 'UTC'), toDateTime64('2024-01-03 00:00:00', 0, 'UTC'))(NULL, number) AS r FROM numbers(3));
SELECT countSparkbar(3, toDateTime64('2024-01-03 00:00:00', 3, 'UTC'), toDateTime64('2024-01-01 00:00:00', 3, 'UTC'))(NULL) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
