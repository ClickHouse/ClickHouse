-- `groupConcat` appends `String` values as they are instead of formatting them as text,
-- and records the per-row offsets only when it has a limit.

-- Bytes that text formats escape are copied verbatim.
SELECT hex(groupConcat('|')(s)) FROM (SELECT arrayJoin(['a\tb', 'c\nd', 'e\'f', 'g\\h', 'i\0j']) AS s);

-- The delimiter is skipped while the result is empty.
SELECT groupConcat(',')(s) FROM (SELECT arrayJoin(['', '', 'a', '', 'b']) AS s);

-- `Nullable` and `LowCardinality` arguments reach the same `String` path.
SELECT groupConcat(',')(s) FROM (SELECT arrayJoin([toNullable('a'), NULL, 'b']) AS s);
SELECT groupConcat(',')(s) FROM (SELECT toLowCardinality(arrayJoin(['a', 'b', 'a'])) AS s);

-- Without a limit the state is only the data; with a limit it also has the row count and offsets.
SELECT hex(arrayReduce('groupConcatState(\',\')', ['ab', 'c']));
SELECT hex(arrayReduce('groupConcatState(\',\', 5)', ['ab', 'c']));

-- Merging states: without a limit the data is appended as a whole, with a limit row by row.
SELECT arrayReduce('groupConcatMerge(\',\')', [arrayReduce('groupConcatState(\',\')', ['a', 'bc']), arrayReduce('groupConcatState(\',\')', ['d', 'e'])]);
SELECT arrayReduce('groupConcatMerge(\',\', 3)', [arrayReduce('groupConcatState(\',\', 3)', ['a', 'bc']), arrayReduce('groupConcatState(\',\', 3)', ['d', 'e'])]);
