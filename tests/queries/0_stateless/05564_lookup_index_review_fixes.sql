-- Regression tests for the lookup index fast paths: each case must give the same result as the
-- regular plan.

SET enable_analyzer = 1;
SET allow_experimental_lookup_index = 1;
-- The lookup fast paths are declined under per-query set and join size limits, which the standard
-- stateless config sets; reset them so the fast paths are exercised.
SET max_rows_in_set = 0, max_bytes_in_set = 0;
SET max_rows_in_join = 0, max_bytes_in_join = 0;
SET join_algorithm = 'direct,hash';

DROP TABLE IF EXISTS lookup_fix_keys;
DROP TABLE IF EXISTS lookup_fix_dim;
DROP TABLE IF EXISTS lookup_fix_fact;

CREATE TABLE lookup_fix_keys (id UInt64, LOOKUP INDEX s (id) TYPE table_set) ENGINE = MergeTree ORDER BY id;
INSERT INTO lookup_fix_keys VALUES (1), (2);

-- `LIMIT AFTER` and `LIMIT UNTIL` restrict the subquery result, so it cannot be replaced by the whole lookup set.
SELECT 'limit after', toUInt64(1) IN (SELECT id FROM lookup_fix_keys LIMIT AFTER id > 100);
SELECT 'limit until', toUInt64(2) IN (SELECT id FROM lookup_fix_keys LIMIT UNTIL id >= 1);

-- A plan that is serialized for remote execution cannot carry the lookup set.
SELECT 'remote', count() FROM remote('127.0.0.{1,2}', currentDatabase(), lookup_fix_keys)
WHERE id IN lookup_fix_keys SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;
SELECT 'remote subquery', count() FROM remote('127.0.0.{1,2}', currentDatabase(), lookup_fix_keys)
WHERE id IN (SELECT id FROM lookup_fix_keys) SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;

CREATE TABLE lookup_fix_dim (id UInt64, value UInt64, LOOKUP INDEX j (id) TYPE table_join) ENGINE = MergeTree ORDER BY id;
INSERT INTO lookup_fix_dim VALUES (1, 10);

CREATE TABLE lookup_fix_fact (id UInt64, payload UInt64) ENGINE = MergeTree ORDER BY payload;
INSERT INTO lookup_fix_fact VALUES (1, 100), (1, 200);

-- `ANY INNER JOIN` emits one row per matching key across both sides, so a repeated left key yields one row.
SELECT 'any inner', count() FROM lookup_fix_fact AS f ANY INNER JOIN lookup_fix_dim AS d ON f.id = d.id
SETTINGS any_join_distinct_right_table_keys = 0;

-- A default-only `ALTER` creates no mutation, so it must still invalidate the cached lookup join.
ALTER TABLE lookup_fix_dim ADD COLUMN v UInt64 DEFAULT 1;
SELECT 'default before', f.payload, d.v FROM lookup_fix_fact AS f LEFT ANY JOIN lookup_fix_dim AS d ON f.id = d.id ORDER BY f.payload;
ALTER TABLE lookup_fix_dim MODIFY COLUMN v UInt64 DEFAULT 2;
SELECT 'default after', f.payload, d.v FROM lookup_fix_fact AS f LEFT ANY JOIN lookup_fix_dim AS d ON f.id = d.id ORDER BY f.payload;

DROP TABLE lookup_fix_fact;
DROP TABLE lookup_fix_dim;
DROP TABLE lookup_fix_keys;
