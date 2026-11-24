-- The second argument of `naturalSortKey` selects the order of strings with non-digit characters.
-- With the value 1, a sequence of non-digit characters is compared as a whole chunk with the chunk at the same position
-- of the other string, and a number is compared only if the chunks before it are equal.
-- It is the order of the PromQL function `sort_by_label`: the orders expected in the scenarios `end of chunk`, `spaces`,
-- `punctuation`, `several chunks` and `number positions` are the ones that Prometheus gives for the same values.
-- In the keys, spaces are shown as `_`, because a key can end with spaces.

-- The key itself: a chunk is followed by two spaces, a space inside a chunk is written as ` A`,
-- and a number is written as its length prefix and its digits without leading zeros.
SELECT '-- keys';
SELECT value, replaceAll(naturalSortKey(value, 1), ' ', '_')
FROM (SELECT arrayJoin(['host-01', 'host2', 'host 3', '4m600', '007', 'host', '12', '']) AS value)
ORDER BY naturalSortKey(value, 1);

-- A chunk is before every longer chunk that starts with it, whatever number follows: `host2` is before `host-01`.
SELECT '-- end of chunk';
SELECT value
FROM (SELECT arrayJoin(['host-01', 'host10', 'hosta1', 'host2', 'host']) AS value)
ORDER BY naturalSortKey(value, 1);

-- The same values without the second argument, as a control: `-` is compared with the number, so `host-01` is before `host2`.
SELECT '-- default mode';
SELECT value
FROM (SELECT arrayJoin(['host10', 'hosta1', 'host2', 'host', 'host-01']) AS value)
ORDER BY naturalSortKey(value);

-- A space inside a chunk is not taken for the end of the chunk, and a chunk that ends with a space
-- is still before a longer chunk that starts with it: `host 3` is before `host -3`.
SELECT '-- spaces';
SELECT value
FROM (SELECT arrayJoin(['host -3', 'host-1', 'host 3', 'host10', 'host  1', 'host2']) AS value)
ORDER BY naturalSortKey(value, 1);

-- A chunk with spaces can be followed by a number and by another chunk: each chunk is escaped and ended on its own.
SELECT '-- chunks with spaces';
SELECT value, replaceAll(naturalSortKey(value, 1), ' ', '_')
FROM (SELECT arrayJoin(['a 1b c2', 'a 01b', 'a b 1 c', 'a1 b', 'a 123456789b']) AS value)
ORDER BY naturalSortKey(value, 1);

-- Characters up to the space keep their order, are after the end of the chunk (`a1`)
-- and before all other characters (`a!1`).
SELECT '-- control characters';
SELECT name, replaceAll(naturalSortKey(value, 1), ' ', '_')
FROM
(
    SELECT arrayJoin([
        ('zero byte', concat('a', char(0), '1')),
        ('char(1)', concat('a', char(1), '1')),
        ('tab', 'a\t1'),
        ('new line', 'a\n1'),
        ('char(31)', concat('a', char(31), '1')),
        ('space', 'a 1'),
        ('exclamation mark', 'a!1'),
        ('no character', 'a1')]) AS pair,
        pair.1 AS name,
        pair.2 AS value
)
ORDER BY naturalSortKey(value, 1);

-- Punctuation is a part of the chunk and decides the order before the number does.
SELECT '-- punctuation';
SELECT value
FROM (SELECT arrayJoin(['host-.&2', 'host--01', 'host1', 'host-03', 'host-1']) AS value)
ORDER BY naturalSortKey(value, 1);

-- The comparison goes chunk by chunk and number by number, and stops at the first difference.
SELECT '-- several chunks';
SELECT value
FROM (SELECT arrayJoin(['1.10.0', '2.1', 'a1bc2', '1.9', 'a1b2', '1.10', 'a1b10']) AS value)
ORDER BY naturalSortKey(value, 1);

-- A number at the start, in the middle and at the end of the string. A string that starts with a number
-- has no chunk before it, so its key starts with the number.
SELECT '-- number positions';
SELECT value, replaceAll(naturalSortKey(value, 1), ' ', '_')
FROM (SELECT arrayJoin(['file10.txt', '10file.txt', 'file.2', '2file.txt', 'file.10', '1file.txt', 'file2.txt']) AS value)
ORDER BY naturalSortKey(value, 1);

-- Numbers are compared by value, as without the second argument: leading zeros are ignored.
SELECT '-- leading zeros';
SELECT value, replaceAll(naturalSortKey(value, 1), ' ', '_')
FROM (SELECT arrayJoin(['file02.txt', 'file003.txt', 'file1.txt', 'file10.txt', 'file0.txt', 'file000.txt']) AS value)
ORDER BY naturalSortKey(value, 1), value;

-- Bytes of multibyte characters are copied as is.
SELECT '-- multibyte characters';
SELECT value, replaceAll(naturalSortKey(value, 1), ' ', '_')
FROM (SELECT arrayJoin(['файл10', 'файл2', 'é 1', 'é1']) AS value)
ORDER BY naturalSortKey(value, 1);

-- The key of an empty string is empty, and the key of `NULL` is `NULL`.
SELECT '-- empty and NULL';
SELECT value, replaceAll(naturalSortKey(value, 1), ' ', '_')
FROM (SELECT arrayJoin(['b', NULL, '', 'a', '1', NULL, '']) AS value)
ORDER BY naturalSortKey(value, 1);

-- A constant and a column give the same key, `LowCardinality` and `Nullable` strings are accepted,
-- the mode can have any unsigned integer type, and a `NULL` mode gives `NULL`.
SELECT '-- types';
SELECT
    naturalSortKey('host2', 1) = naturalSortKey(materialize('host2'), 1) AS constant_as_column,
    naturalSortKey(toLowCardinality('host2'), 1) = naturalSortKey('host2', 1) AS low_cardinality,
    toTypeName(naturalSortKey('host2', 1)) AS string_type,
    toTypeName(naturalSortKey(toNullable('host2'), 1)) AS nullable_type,
    naturalSortKey('host2', toUInt64(1)) = naturalSortKey('host2', 1) AS wide_mode_type,
    naturalSortKey('host2', NULL) AS null_mode;

-- The value 0 is the same as no second argument.
SELECT '-- mode 0';
SELECT countIf(naturalSortKey(value, 0) != naturalSortKey(value)), count()
FROM (SELECT arrayJoin(['host-01', 'host2', 'host 3', '4m600', '007', 'host', '12', '', 'a\t1', 'файл10']) AS value);

-- The mode must be the constant 0 or 1.
SELECT '-- bad arguments';
SELECT naturalSortKey('host2', 2); -- { serverError BAD_ARGUMENTS }
SELECT naturalSortKey('host2', materialize(1)); -- { serverError ILLEGAL_COLUMN }
SELECT naturalSortKey('host2', '1'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT naturalSortKey('host2', -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT naturalSortKey('host2', 1, 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
