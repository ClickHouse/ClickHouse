-- Tags: no-ordinary-database, no-fasttest
-- no-fasttest: `UNIQUE KEY` needs RocksDB.
-- A `UNIQUE KEY` table reads the parts visible at its internal snapshot, and the commit of an insert
-- changes that visibility without changing the active parts. Its modification hash is not computed, so
-- the consistent query cache does not store a result read from it.

SET enable_unique_key = 1;

DROP TABLE IF EXISTS uk_05443;
CREATE TABLE uk_05443 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k UNIQUE KEY (k);
INSERT INTO uk_05443 VALUES (1, 1);

SELECT modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 'uk_05443';

SELECT sum(v) FROM uk_05443 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1, log_comment = '05443';
SELECT count() FROM system.query_cache WHERE query LIKE '%uk_05443%' AND query LIKE '%05443%';

INSERT INTO uk_05443 VALUES (1, 2);
SELECT sum(v) FROM uk_05443 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1, log_comment = '05443';

DROP TABLE uk_05443;
