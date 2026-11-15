-- https://github.com/ClickHouse/ClickHouse/issues/126202
-- re2 accepts `_` and non-ASCII letters in a group name, but the regexp analysis took such a name
-- as literal text that the subject must contain, so these functions missed matches.

SELECT match('id=42', 'id=(?P<userid>\\d+)'), match('id=42', 'id=(?P<user_id>\\d+)'), match('id=42', 'id=(?<user_id>\\d+)'), match('abc', '(?P<имя>abc)');
SELECT extract('id=42', 'id=(?P<user_id>\\d+)'), extractAll('id=42 id=7', 'id=(?P<user_id>\\d+)'), countMatches('id=42 id=7', 'id=(?P<user_id>\\d+)');
SELECT match('abc', '(?P<_x>ab)c'), extract('xabcx', '(?<имя>abc)');
SELECT regexpExtract('ab', '(?P<user_id>a)(?P<имя>b)', 2), splitByRegexp('(?P<sep_char>,)', 'a,b,c');
SELECT match('xyz', 'id=(?P<user_id>\\d+)'), match('id=', 'id=(?P<user_id>\\d+)');
SELECT groupArray(match(s, 'id=(?P<user_id>\\d+)')), groupArray(extract(s, 'id=(?P<user_id>\\d+)')) FROM (SELECT arrayJoin(['id=1', 'x', 'id=22']) AS s);

-- re2 still rejects invalid names, look-behind and the `(?'name'...)` syntax.
SELECT match('x', '(?P<a-b>x)'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('x', '(?P<€>x)'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('x', '(?P<>x)'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('x', '(?P<name'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('ab', '(?<=a)b'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('x', '(?\'name\'x)'); -- { serverError CANNOT_COMPILE_REGEXP }

-- The n-gram skip index and `merge` use the same analysis.
DROP TABLE IF EXISTS t_named_group;
CREATE TABLE t_named_group (s String, INDEX i s TYPE ngrambf_v1(3, 512, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_named_group VALUES ('id=1'), ('foo'), ('id=22'), ('bar');
SELECT count() FROM t_named_group WHERE match(s, 'id=(?P<user_id>\\d+)') SETTINGS force_data_skipping_indices = 'i';
SELECT count() FROM merge(currentDatabase(), '^t_(?P<table_name>named_group)$');
DROP TABLE t_named_group;
