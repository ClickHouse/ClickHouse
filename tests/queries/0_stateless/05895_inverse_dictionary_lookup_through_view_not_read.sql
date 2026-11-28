-- Tags: no-parallel-replicas
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- `optimize_inverse_dictionary_lookup` must not load the dictionary of a view column that it cannot rewrite, and must not
-- look into a view for a query that is only analyzed, such as the query of `CREATE VIEW`: without the optimization,
-- neither reads the dictionary.

SET enable_analyzer = 1;
SET analyzer_inline_views = 0;

DROP VIEW IF EXISTS not_read_w;
DROP VIEW IF EXISTS not_read_v;
DROP VIEW IF EXISTS not_read_no_key_v;
DROP DICTIONARY IF EXISTS not_read_dict;
DROP TABLE IF EXISTS not_read_data;

CREATE TABLE not_read_data (id UInt64) ENGINE = MergeTree ORDER BY id;

-- The source of the dictionary does not exist, so loading the dictionary fails.
CREATE DICTIONARY not_read_dict (id UInt64, name String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'not_read_no_such_table')) LAYOUT(HASHED()) LIFETIME(0);

-- The view does not project the key of `dictGet`, so the filter cannot be rewritten.
CREATE VIEW not_read_no_key_v AS SELECT dictGetString('not_read_dict', 'name', id) AS name FROM not_read_data;

SELECT count() FROM not_read_no_key_v WHERE name = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT count() FROM not_read_no_key_v WHERE name = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1;

CREATE VIEW not_read_v AS SELECT id, dictGetString('not_read_dict', 'name', id) AS name FROM not_read_data;

SET optimize_inverse_dictionary_lookup = 1;
CREATE VIEW not_read_w AS SELECT id FROM not_read_v WHERE name = 'x';

SELECT status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'not_read_dict';

DROP VIEW not_read_w;
DROP VIEW not_read_v;
DROP VIEW not_read_no_key_v;
DROP DICTIONARY not_read_dict;
DROP TABLE not_read_data;
