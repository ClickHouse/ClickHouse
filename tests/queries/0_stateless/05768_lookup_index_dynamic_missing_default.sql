-- A column absent from an old part is filled by evaluating its default on every read. Such a default
-- can depend on mutable state (here a dictionary), so the lookup caches must not serve a stale value.

SET enable_analyzer = 1;
SET allow_experimental_lookup_index = 1;
SET max_rows_in_set = 0, max_bytes_in_set = 0;
SET max_rows_in_join = 0, max_bytes_in_join = 0;
SET join_algorithm = 'direct,hash';

DROP TABLE IF EXISTS lookup_dyn_fact;
DROP TABLE IF EXISTS lookup_dyn_dim;
DROP TABLE IF EXISTS lookup_dyn_keys;
DROP DICTIONARY IF EXISTS lookup_dyn_dict;
DROP TABLE IF EXISTS lookup_dyn_source;

CREATE TABLE lookup_dyn_source (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lookup_dyn_source VALUES (1, 10);

CREATE DICTIONARY lookup_dyn_dict (id UInt64, v UInt64)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'lookup_dyn_source'))
LIFETIME(0)
LAYOUT(FLAT());

-- `table_join`: the payload column is missing from the part and comes from a `dictGet` default.
CREATE TABLE lookup_dyn_dim (id UInt64, LOOKUP INDEX j (id) TYPE table_join) ENGINE = MergeTree ORDER BY id;
INSERT INTO lookup_dyn_dim VALUES (1);
ALTER TABLE lookup_dyn_dim ADD COLUMN v UInt64 DEFAULT dictGet('lookup_dyn_dict', 'v', id);

CREATE TABLE lookup_dyn_fact (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lookup_dyn_fact VALUES (1);

-- `table_set`: the key column itself is missing from the part.
CREATE TABLE lookup_dyn_keys (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lookup_dyn_keys VALUES (1);
ALTER TABLE lookup_dyn_keys ADD COLUMN k UInt64 DEFAULT dictGet('lookup_dyn_dict', 'v', id);
ALTER TABLE lookup_dyn_keys ADD LOOKUP INDEX s (k) TYPE table_set;

SELECT 'join before', d.v FROM lookup_dyn_fact AS f INNER JOIN lookup_dyn_dim AS d ON f.id = d.id;
SELECT 'join before', d.v FROM lookup_dyn_fact AS f INNER JOIN lookup_dyn_dim AS d ON f.id = d.id;
SELECT 'set before', toUInt64(10) IN (SELECT k FROM lookup_dyn_keys), toUInt64(20) IN (SELECT k FROM lookup_dyn_keys);
SELECT 'set before', toUInt64(10) IN (SELECT k FROM lookup_dyn_keys), toUInt64(20) IN (SELECT k FROM lookup_dyn_keys);

ALTER TABLE lookup_dyn_source UPDATE v = 20 WHERE id = 1 SETTINGS mutations_sync = 2;
SYSTEM RELOAD DICTIONARY lookup_dyn_dict;

SELECT 'plain after', v FROM lookup_dyn_dim;
SELECT 'join after', d.v FROM lookup_dyn_fact AS f INNER JOIN lookup_dyn_dim AS d ON f.id = d.id;
SELECT 'set after', toUInt64(10) IN (SELECT k FROM lookup_dyn_keys), toUInt64(20) IN (SELECT k FROM lookup_dyn_keys);

DROP TABLE lookup_dyn_fact;
DROP TABLE lookup_dyn_dim;
DROP TABLE lookup_dyn_keys;
DROP DICTIONARY lookup_dyn_dict;
DROP TABLE lookup_dyn_source;
