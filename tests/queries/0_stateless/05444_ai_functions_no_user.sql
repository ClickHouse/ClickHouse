-- Tags: no-parallel
-- no-parallel: creates a global named collection and a global user with fixed names

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
CREATE TABLE t_embed (x String, CONSTRAINT c CHECK length(aiEmbed(x, 'm', map('credentials', 'nc_05444'))) > 0) ENGINE = MergeTree ORDER BY x;
CREATE TABLE b_embed (x String) ENGINE = Buffer(currentDatabase(), t_embed, 1, 1, 1, 1, 1, 1, 1);
INSERT INTO b_embed VALUES ('a'); -- { serverError ACCESS_DENIED }
CREATE TABLE t_similarity (x String, CONSTRAINT c CHECK aiSimilarity(x, x, 'm', map('credentials', 'nc_05444')) > 0) ENGINE = MergeTree ORDER BY x;
CREATE TABLE b_similarity (x String) ENGINE = Buffer(currentDatabase(), t_similarity, 1, 1, 1, 1, 1, 1, 1);
INSERT INTO b_similarity VALUES ('a'); -- { serverError ACCESS_DENIED }
-- The failure is reported in `system.warnings`.
SELECT count() > 0 FROM system.warnings WHERE message LIKE 'A background operation%AI function%';

-- The user is checked before the credentials: an inaccessible and an absent collection fail with the same error.
DROP USER IF EXISTS u_05444;
CREATE USER u_05444 IDENTIFIED WITH no_password;
GRANT CREATE TABLE, INSERT, SELECT ON * TO u_05444;
GRANT TABLE ENGINE ON MergeTree, TABLE ENGINE ON Buffer TO u_05444;
EXECUTE AS u_05444 CREATE TABLE t_hidden (x String, CONSTRAINT c CHECK aiFilter(x, 'c', map('credentials', 'nc_05444', 'model', 'm'))) ENGINE = MergeTree ORDER BY x;
EXECUTE AS u_05444 CREATE TABLE b_hidden (x String) ENGINE = Buffer(currentDatabase(), t_hidden, 1, 1, 1, 1, 1, 1, 1);
EXECUTE AS u_05444 INSERT INTO b_hidden SELECT 'a'; -- { serverError ACCESS_DENIED }
EXECUTE AS u_05444 CREATE TABLE t_absent (x String, CONSTRAINT c CHECK aiFilter(x, 'c', map('credentials', 'absent_05444', 'model', 'm'))) ENGINE = MergeTree ORDER BY x;
EXECUTE AS u_05444 CREATE TABLE b_absent (x String) ENGINE = Buffer(currentDatabase(), t_absent, 1, 1, 1, 1, 1, 1, 1);
EXECUTE AS u_05444 INSERT INTO b_absent SELECT 'a'; -- { serverError ACCESS_DENIED }

-- Mutations are executed in the background, so AI functions in them are rejected when submitted.
CREATE TABLE t_update (x String, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_update VALUES ('a', '');
ALTER TABLE t_update UPDATE y = aiGenerate(x, map('credentials', 'nc_05444', 'model', 'm')) WHERE 1 SETTINGS mutations_sync = 0, allow_nondeterministic_mutations = 1; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_update UPDATE y = 'e' WHERE length(aiEmbed(x, 'm', map('credentials', 'nc_05444'))) > 0 SETTINGS mutations_sync = 0; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_update DELETE WHERE aiFilter(x, 'c', map('credentials', 'nc_05444', 'model', 'm')) SETTINGS mutations_sync = 0, allow_nondeterministic_mutations = 1; -- { serverError NOT_IMPLEMENTED }
DROP FUNCTION IF EXISTS udf_05444;
CREATE FUNCTION udf_05444 AS x -> aiGenerate(x, map('credentials', 'nc_05444', 'model', 'm'));
ALTER TABLE t_update UPDATE y = udf_05444(x) WHERE 1 SETTINGS mutations_sync = 0, allow_nondeterministic_mutations = 1; -- { serverError NOT_IMPLEMENTED }
DROP FUNCTION udf_05444;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_update';
ALTER TABLE t_update UPDATE y = 'z' WHERE 1 SETTINGS mutations_sync = 1;
SELECT x, y FROM t_update;

-- `DEFAULT` and `MATERIALIZED` columns are evaluated by the insert, which has a user.
-- The request fails (unreachable endpoint) and returns an empty string instead of throwing.
SET ai_function_throw_on_error = 0, ai_function_max_retries = 0;

-- Updating a column recomputes a `MATERIALIZED` column that depends on it, which is rejected the same way.
CREATE TABLE t_materialized (k UInt8, x String, m String MATERIALIZED aiGenerate(x, map('credentials', 'nc_05444', 'model', 'm'))) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_materialized (k, x) VALUES (1, 'a');
ALTER TABLE t_materialized UPDATE x = 'b' WHERE 1 SETTINGS mutations_sync = 0, allow_nondeterministic_mutations = 1; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_materialized';

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

-- A lightweight update is evaluated by the query, which has a user.
CREATE TABLE t_lwu (x String, y String) ENGINE = MergeTree ORDER BY x
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_lwu VALUES ('a', 'old');
UPDATE t_lwu SET y = aiGenerate(x, map('credentials', 'nc_05444', 'model', 'm')) WHERE 1 SETTINGS enable_lightweight_update = 1;
SELECT x, y FROM t_lwu;

DROP TABLE b;
DROP TABLE t;
DROP TABLE b_embed;
DROP TABLE t_embed;
DROP TABLE b_similarity;
DROP TABLE t_similarity;
DROP TABLE t_update;
DROP TABLE t_default;
DROP TABLE t_lwu;
DROP TABLE t_materialized;
DROP TABLE b_hidden;
DROP TABLE t_hidden;
DROP TABLE b_absent;
DROP TABLE t_absent;
DROP USER u_05444;
DROP NAMED COLLECTION nc_05444;
