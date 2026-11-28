-- `regexp_split_to_array(s, regexp)` of PostgreSQL is `splitByRegexp(regexp, s)`.
SELECT regexp_split_to_array('a1b2c', '[0-9]');
SELECT REGEXP_SPLIT_TO_ARRAY('hello world', '\\s+');
SELECT regexp_split_to_array('', ','), regexp_split_to_array('abc', ''), regexp_split_to_array('a,b,', ',');
SELECT regexp_split_to_array(s, ',') FROM (SELECT arrayJoin(['a,b', 'c']) AS s);
SELECT regexp_split_to_array(s, '[,;]') = splitByRegexp('[,;]', s) FROM (SELECT arrayJoin(['a,b;c', 'd', '']) AS s);
SELECT regexp_split_to_array(NULL, ','), toTypeName(regexp_split_to_array('a', ','));
SELECT regexp_split_to_array('a'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
