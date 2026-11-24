-- Tags: no-fasttest, no-parallel, long
-- Tag no-fasttest: Test runtime is > 6 sec
-- Tag long: Test runtime is > 6 sec
-- Tag no-parallel: Messes with internal cache

-- An insert into a full query cache evicts the stale entries, and only them. The stale entries are found via an index on the expiry
-- time, so the entry which expires first is deliberately not the entry which was inserted first.

SYSTEM CLEAR QUERY CACHE;

-- The cache is full for the current user after two entries
SET query_cache_max_entries = 2;

SELECT 'long-lived' SETTINGS use_query_cache = true, query_cache_ttl = 600;
SELECT 'short-lived' SETTINGS use_query_cache = true, query_cache_ttl = 6;

SELECT 'The cache is full and no entry is stale, expect that the new entry is not cached';
SELECT 'new' SETTINGS use_query_cache = true, query_cache_ttl = 600;
SELECT substring(query, 1, position(query, ' SETTINGS') - 1), stale FROM system.query_cache ORDER BY query;

-- Wait until the short-lived entry is expired
SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;
SELECT sleep(1) FORMAT Null;

SELECT 'The cache is full and one entry is stale, expect that the new entry replaces the stale entry';
SELECT 'new' SETTINGS use_query_cache = true, query_cache_ttl = 600;
SELECT substring(query, 1, position(query, ' SETTINGS') - 1), stale FROM system.query_cache ORDER BY query;

SELECT 'Nothing is stale again, expect that another new entry is not cached';
SELECT 'another new' SETTINGS use_query_cache = true, query_cache_ttl = 600;
SELECT substring(query, 1, position(query, ' SETTINGS') - 1), stale FROM system.query_cache ORDER BY query;

SYSTEM CLEAR QUERY CACHE;
