-- Replicating an array of aggregate states gathers its elements in chunks; the chunks must keep sharing the states.
-- Each state takes about 8 KB, so copying it for every one of 100000 rows would exceed the memory limit.
SET max_block_size = 100000, max_memory_usage = 200000000;

SELECT count(), sum(length(a)) FROM (SELECT materialize([arrayReduce('groupArrayState', range(toUInt64(1000)))]) AS a FROM numbers(100000));
SELECT count(), sum(length(a)) FROM (SELECT materialize([[arrayReduce('groupArrayState', range(toUInt64(1000)))]]) AS a FROM numbers(100000));
SELECT count(), sum(length(a)) FROM (SELECT materialize([(arrayReduce('groupArrayState', range(toUInt64(1000))), 1)]) AS a FROM numbers(100000));

SELECT length(finalizeAggregation(a[1])), arraySum(finalizeAggregation(a[1]))
FROM (SELECT number, materialize([arrayReduce('groupArrayState', range(toUInt64(1000)))]) AS a FROM numbers(100000))
WHERE number IN (0, 70000, 99999);
