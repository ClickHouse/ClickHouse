-- A constant-key lookup of a key that the `ALIAS` masks out folds to a constant,
-- so a filter on it reads no rows instead of scanning the whole table.

SET enable_parallel_replicas = 0;
SET optimize_map_alias_pushdown = 1;

DROP TABLE IF EXISTS t_denied_key;
DROP DICTIONARY IF EXISTS d_denied_key_policy;
DROP TABLE IF EXISTS t_denied_key_policy;

CREATE TABLE t_denied_key_policy (user String, blocked Array(String)) ENGINE = MergeTree ORDER BY user;
INSERT INTO t_denied_key_policy VALUES (currentUser(), ['ssn']);

CREATE DICTIONARY d_denied_key_policy (user String, blocked Array(String))
PRIMARY KEY user
SOURCE(CLICKHOUSE(TABLE 't_denied_key_policy'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(0);

CREATE TABLE t_denied_key
(
    ts UInt32,
    raw_attrs Map(String, String),
    attrs_static Map(String, String) ALIAS mapFilter((k, v) -> k != 'ssn', raw_attrs),
    attrs_dict Map(String, String) ALIAS mapFilter(
        (k, v) -> NOT has(dictGetOrDefault('d_denied_key_policy', 'blocked', currentUser(), CAST([], 'Array(String)')), k),
        raw_attrs)
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS index_granularity = 100;

INSERT INTO t_denied_key SELECT number, map('ssn', toString(number % 10), 'service', 'svc' || toString(number % 3)) FROM numbers(10000);

SELECT '--- denied key reads no rows';
SELECT count() FROM t_denied_key WHERE attrs_static['ssn'] = '1' SETTINGS max_rows_to_read = 1;
SELECT count() FROM t_denied_key WHERE attrs_dict['ssn'] = '1' SETTINGS max_rows_to_read = 1;
SELECT count() FROM t_denied_key WHERE has(attrs_dict, 'ssn') SETTINGS max_rows_to_read = 1;
SELECT count() FROM t_denied_key WHERE mapContainsKey(attrs_dict, 'ssn') SETTINGS max_rows_to_read = 1;
SELECT ts FROM t_denied_key WHERE attrs_dict['ssn'] = '1' SETTINGS max_rows_to_read = 1;

SELECT '--- without the pass the same filter scans the table';
SELECT count() FROM t_denied_key WHERE attrs_dict['ssn'] = '1' SETTINGS optimize_map_alias_pushdown = 0, max_rows_to_read = 1; -- { serverError TOO_MANY_ROWS }

SELECT '--- results match the unoptimized ALIAS';
SELECT count() FROM t_denied_key WHERE attrs_dict['ssn'] = '';
SELECT count() FROM t_denied_key WHERE attrs_dict['ssn'] = '' SETTINGS optimize_map_alias_pushdown = 0;
SELECT count() FROM t_denied_key WHERE attrs_dict['service'] = 'svc1';
SELECT count() FROM t_denied_key WHERE attrs_dict['service'] = 'svc1' SETTINGS optimize_map_alias_pushdown = 0;

SELECT '--- after the policy allows the key, it is read again';
TRUNCATE TABLE t_denied_key_policy;
INSERT INTO t_denied_key_policy VALUES (currentUser(), []);
SYSTEM RELOAD DICTIONARY d_denied_key_policy;
SELECT count() FROM t_denied_key WHERE attrs_dict['ssn'] = '1';
SELECT count() FROM t_denied_key WHERE attrs_static['ssn'] = '1' SETTINGS max_rows_to_read = 1;

DROP TABLE t_denied_key;
DROP DICTIONARY d_denied_key_policy;
DROP TABLE t_denied_key_policy;
