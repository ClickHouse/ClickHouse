-- Tags: no-fasttest
-- has/indexOf/countEqual over Array(FixedString(N)) never matched a String needle shorter than N:
-- the common type of the pair is String, and the cast kept the element's NUL padding, while `equals`
-- pads the shorter operand and matches.

SELECT has([toFixedString('ab', 4)], 'ab');
SELECT has(materialize([toFixedString('ab', 4)]), 'ab');
SELECT indexOf([toFixedString('ab', 4)], 'ab');
SELECT countEqual([toFixedString('ab', 4)], 'ab');
SELECT has([toFixedString('ab', 4)], toFixedString('ab', 2));
SELECT has([toFixedString('ab', 2)], 'ab');
SELECT has(['abc'], 'abc');
SELECT has([toFixedString('', 4)], '');
SELECT 'ab' IN [toFixedString('ab', 4)];
SELECT arrayFirst(x -> x = 'ab', [toFixedString('ab', 4)]) = 'ab';
