-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A `QueryRunner` job in cluster mode is sent to the remote server as an initial query: it has not
-- been through the rules anywhere, so it keeps its `query_rules` and the remote applies them.

DROP RULE IF EXISTS rule_runner_reject;
CREATE RULE rule_runner_reject AS (SELECT 777) REJECT WITH 'rejected by rule_runner_reject';

DROP TABLE IF EXISTS runner;
CREATE TABLE runner (query String, database String, settings Map(String, String)) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', mode = 'synchronous';
-- A token generated per run keeps the jobs of an earlier run in the same database out of the lookup below.
CREATE TEMPORARY TABLE run AS SELECT toString(generateUUIDv4()) AS id;
INSERT INTO runner SELECT query, currentDatabase(), map('log_comment', 'runner_' || name || '_' || (SELECT id FROM run), 'query_rules', rules)
FROM values('name String, query String, rules String',
    ('rejected', 'SELECT 777', 'rule_runner_reject'),
    ('not_matching', 'SELECT 778', 'rule_runner_reject'),
    ('missing_rule', 'SELECT 778', 'no_such_rule_runner'));
SYSTEM FLUSH LOGS query_log;
SELECT replaceOne(log_comment, '_' || (SELECT id FROM run), '') AS job, type, errorCodeToName(exception_code) FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND endsWith(log_comment, '_' || (SELECT id FROM run)) AND is_internal AND type != 'QueryStart'
ORDER BY job;
DROP TABLE runner;
DROP RULE rule_runner_reject;
