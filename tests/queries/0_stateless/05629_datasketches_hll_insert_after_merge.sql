-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- `arrayReduceInRanges` merges complete blocks of rows and then inserts the remaining rows into
-- the same state. Values inserted after a merge must not be lost, and the state must stay
-- consistent across serialization (the `remote` queries ship partial states over the network).

SELECT arrayReduceInRanges('uniqHLL', [(1, 65)], range(65));
SELECT arrayReduceInRanges('uniqHLL', [(1, 65)], arrayMap(x -> toString(x), range(65)));
SELECT cardinalityFromHLL(arrayReduceInRanges('serializedHLL', [(1, 65)], range(65))[1]);
SELECT cardinalityFromHLL(arrayReduceInRanges('serializedHLL', [(1, 65)], arrayMap(x -> toString(x), range(65)))[1]);

SELECT uniqHLLMerge(s) FROM (SELECT arrayJoin(arrayReduceInRanges('uniqHLLState', [(1, 65)], range(65))) AS s);
SELECT uniqHLLMerge(s) FROM remote('127.0.0.{1,2}', view(SELECT arrayJoin(arrayReduceInRanges('uniqHLLState', [(1, 65)], range(65))) AS s));
SELECT cardinalityFromHLL(serializedHLLMerge(s)) FROM remote('127.0.0.{1,2}', view(SELECT arrayJoin(arrayReduceInRanges('serializedHLLState', [(1, 65)], arrayMap(x -> toString(x), range(65)))) AS s));
