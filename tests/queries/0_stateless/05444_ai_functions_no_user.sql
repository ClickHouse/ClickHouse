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

-- `DEFAULT` and `MATERIALIZED` columns are evaluated by the insert, which has a user.
-- The request fails (unreachable endpoint) and returns an empty string instead of throwing.
SET ai_function_throw_on_error = 0, ai_function_max_retries = 0;

CREATE TABLE t_default
(
    x String,
    d String DEFAULT aiGenerate(x, map('credentials', 'nc_05444', 'model', 'm')),
    m String MATERIALIZED aiGenerate(x, map('credentials', 'nc_05444', 'model', 'm'))
)
ENGINE = MergeTree ORDER BY x;
INSERT INTO t_default (x) VALUES ('a');
INSERT INTO t_default (x) VALUES ('b');
SELECT x, d, m FROM t_default ORDER BY x;

-- A merge does not evaluate them again.
OPTIMIZE TABLE t_default FINAL;
SELECT x, d, m FROM t_default ORDER BY x;

DROP TABLE b;
DROP TABLE t;
DROP TABLE t_update;
DROP TABLE t_default;
DROP NAMED COLLECTION nc_05444;
