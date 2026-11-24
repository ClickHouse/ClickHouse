-- `string_to_array(s, delimiter[, null_string])` of PostgreSQL. The expected values are the ones
-- PostgreSQL 14 returns, except for a `NULL` string: it gives an empty array, because an array cannot be `NULL`.
SELECT string_to_array('a,b,c', ','), STRING_TO_ARRAY('a,b,', ','), string_to_array('a--b', '--');

-- An empty string gives an empty array, and with an empty delimiter the whole string is the only element.
SELECT string_to_array('', ','), string_to_array('', NULL), string_to_array('abc', '');

-- A `NULL` delimiter splits the string into characters.
SELECT string_to_array('abc', NULL), string_to_array('aяb', NULL);

-- The elements equal to `null_string` are replaced by `NULL`, unless it is `NULL`.
SELECT string_to_array('a,*,c', ',', '*'), string_to_array('a,*,c', ',', NULL), string_to_array('a,,c', ',', '');
SELECT string_to_array('*', NULL, '*'), string_to_array('abc', '', 'abc');

SELECT string_to_array(NULL, ','), string_to_array(NULL, ',', '*');
SELECT toTypeName(string_to_array('a', ',')), toTypeName(string_to_array('a', ',', 'x')), toTypeName(string_to_array('a', NULL));
SELECT string_to_array(s, ',') FROM (SELECT arrayJoin(['a,b', '', 'c']) AS s);
SELECT s, d, string_to_array(s, d) FROM VALUES('s Nullable(String), d Nullable(String)', ('a,b', ','), (NULL, ','), ('ab', NULL), ('ab', ''));
SELECT string_to_array(s, ',', n) FROM VALUES('s String, n Nullable(String)', ('a,b', 'a'), ('a,b', NULL), ('a,b', 'b'));
SELECT string_to_array('a,b', ',') = splitByString(',', 'a,b');

SELECT string_to_array(1, ','); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT string_to_array('a'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT string_to_array('a', ',', '*', 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
