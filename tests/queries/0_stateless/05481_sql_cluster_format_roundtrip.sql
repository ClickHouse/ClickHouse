-- A map must format as `{}`, not `[]`, and `shard` / `replica` as property names must stay quoted
-- so they are not re-parsed as the `SHARD` / `REPLICA` keywords. Debug builds abort on
-- `Inconsistent AST formatting` before this SELECT if the round-trip fails.

SELECT formatQuerySingleLine('CREATE CLUSTER c (user = {}, SHARD (host = \'127.0.0.1\', port = 9000))');
SELECT formatQuerySingleLine('CREATE CLUSTER c (`shard` = 1, SHARD (host = \'127.0.0.1\', port = 9000))');
SELECT formatQuerySingleLine('CREATE CLUSTER c (SHARD (`replica` = 1, host = \'127.0.0.1\', port = 9000))');

EXPLAIN AST CREATE CLUSTER c (user = {}, SHARD (host = '127.0.0.1', port = 9000)) FORMAT Null;
EXPLAIN AST CREATE CLUSTER c (`shard` = 1, SHARD (host = '127.0.0.1', port = 9000)) FORMAT Null;
EXPLAIN AST CREATE CLUSTER c (SHARD (`replica` = 1, host = '127.0.0.1', port = 9000)) FORMAT Null;
