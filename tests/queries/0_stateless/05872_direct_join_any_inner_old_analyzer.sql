-- `ANY INNER JOIN` emits one row per matching key across both sides, which a direct join does not implement,
-- so it must not be chosen: a repeated left key yields one row, as with the `hash` algorithm.

DROP DICTIONARY IF EXISTS direct_any_inner_dict;
DROP TABLE IF EXISTS direct_any_inner_dim;
DROP TABLE IF EXISTS direct_any_inner_fact;

CREATE TABLE direct_any_inner_dim (id UInt64, value UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO direct_any_inner_dim VALUES (1, 10);

CREATE DICTIONARY direct_any_inner_dict (id UInt64, value UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'direct_any_inner_dim')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE direct_any_inner_fact (id UInt64, payload UInt64) ENGINE = MergeTree ORDER BY payload;
INSERT INTO direct_any_inner_fact VALUES (1, 100), (1, 200);

SET any_join_distinct_right_table_keys = 0;

SELECT 'old analyzer', count()
FROM direct_any_inner_fact AS f ANY INNER JOIN direct_any_inner_dict AS d ON f.id = d.id
SETTINGS enable_analyzer = 0, join_algorithm = 'hash';

SELECT 'old analyzer', count()
FROM direct_any_inner_fact AS f ANY INNER JOIN direct_any_inner_dict AS d ON f.id = d.id
SETTINGS enable_analyzer = 0, join_algorithm = 'direct,hash';

SELECT 'analyzer', count()
FROM direct_any_inner_fact AS f ANY INNER JOIN direct_any_inner_dict AS d ON f.id = d.id
SETTINGS enable_analyzer = 1, join_algorithm = 'direct,hash';

DROP DICTIONARY direct_any_inner_dict;
DROP TABLE direct_any_inner_dim;
DROP TABLE direct_any_inner_fact;
