-- A JOIN that compares a Bool typed path of a JSON key as UInt8 keeps the SHARED REGEXP rules of the keys' common type.

SET enable_json_type = 1, enable_analyzer = 1;

DROP TABLE IF EXISTS t_json_rules_bool_key;
DROP TABLE IF EXISTS t_json_rules_int_key;
CREATE TABLE t_json_rules_bool_key (k JSON(a Nullable(Bool), SHARED REGEXP '^p')) ENGINE = Memory;
CREATE TABLE t_json_rules_int_key (k JSON(a UInt8, SHARED REGEXP '^p')) ENGINE = Memory;
INSERT INTO t_json_rules_bool_key VALUES ('{"a":true,"p":1}'), ('{"a":true,"p":2}');
INSERT INTO t_json_rules_int_key VALUES ('{"a":1,"p":1}'), ('{"a":2,"p":1}');

SELECT 'common type', toTypeName([l.k, r.k]) FROM t_json_rules_bool_key AS l, t_json_rules_int_key AS r LIMIT 1;
SELECT 'join', count() FROM t_json_rules_bool_key AS l JOIN t_json_rules_int_key AS r ON l.k = r.k;
SELECT 'key cast with rules', countIf(explain LIKE '%JSON(a Nullable(UInt8), SHARED REGEXP%') > 0, countIf(explain LIKE '%JSON(a Nullable(UInt8))%')
FROM (EXPLAIN actions = 1 SELECT count() FROM t_json_rules_bool_key AS l JOIN t_json_rules_int_key AS r ON l.k = r.k);

DROP TABLE t_json_rules_bool_key;
DROP TABLE t_json_rules_int_key;
