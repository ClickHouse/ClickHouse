-- Tags: no-parallel
-- no-parallel: creates a global named collection with a fixed name

-- AI functions are not executed with a context without a user (`Buffer` flushes, mutations).
-- No request is made: the check runs before the provider is contacted.

DROP NAMED COLLECTION IF EXISTS nc_05444;
CREATE NAMED COLLECTION nc_05444 AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    api_key = 'fake-key';

CREATE TABLE t (x String, CONSTRAINT c CHECK aiFilter(x, 'c', map('credentials', 'nc_05444', 'model', 'm'))) ENGINE = MergeTree ORDER BY x;
CREATE TABLE b (x String) ENGINE = Buffer(currentDatabase(), t, 1, 1, 1, 1, 1, 1, 1);
INSERT INTO b VALUES ('a'); -- { serverError ACCESS_DENIED }

CREATE TABLE t_update (x String, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_update VALUES ('a', '');
ALTER TABLE t_update UPDATE y = aiGenerate(x, map('credentials', 'nc_05444', 'model', 'm')) WHERE 1 SETTINGS mutations_sync = 1; -- { serverError UNFINISHED }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_update' SYNC FORMAT Null;

DROP TABLE b;
DROP TABLE t;
DROP TABLE t_update;
DROP NAMED COLLECTION nc_05444;
