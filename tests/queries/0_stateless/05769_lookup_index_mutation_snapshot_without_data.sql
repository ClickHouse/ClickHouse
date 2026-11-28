-- `MutationsInterpreter` analyzes the mutation with a storage snapshot taken without data parts.
-- The lookup fast path must not be used with such a snapshot.

SET enable_analyzer = 1;
SET allow_experimental_lookup_index = 1;
SET max_rows_in_set = 0, max_bytes_in_set = 0;
SET mutations_sync = 2;

DROP TABLE IF EXISTS lookup_mut_keys;
DROP TABLE IF EXISTS lookup_mut_data;

CREATE TABLE lookup_mut_keys (id UInt64, LOOKUP INDEX s (id) TYPE table_set) ENGINE = MergeTree ORDER BY id;
INSERT INTO lookup_mut_keys VALUES (1), (2), (3);

CREATE TABLE lookup_mut_data (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lookup_mut_data VALUES (1, 0), (2, 0), (5, 0);

ALTER TABLE lookup_mut_data UPDATE v = 1 WHERE id IN lookup_mut_keys;
ALTER TABLE lookup_mut_data UPDATE v = v + 10 WHERE id IN (SELECT id FROM lookup_mut_keys);
SELECT 'data', id, v FROM lookup_mut_data ORDER BY id;

ALTER TABLE lookup_mut_keys DELETE WHERE id IN lookup_mut_keys AND id > 2;
ALTER TABLE lookup_mut_keys DELETE WHERE id IN (SELECT id FROM lookup_mut_keys WHERE id = 1);
SELECT 'keys', id FROM lookup_mut_keys ORDER BY id;

DROP TABLE lookup_mut_data;
DROP TABLE lookup_mut_keys;
