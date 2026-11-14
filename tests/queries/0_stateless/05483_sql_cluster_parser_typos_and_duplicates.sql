-- `SHARD` / `REPLICA` without `(` used to drop the keyword and keep parsing, so a typo
-- silently changed the topology. Duplicate keys at one level used to leave the last
-- inherited value winning over a more specific one.

CREATE CLUSTER t05483_missing_replica_paren (SHARD (REPLICA host = '127.0.0.1', REPLICA host = '127.0.0.2')); -- { clientError SYNTAX_ERROR }

CREATE CLUSTER t05483_missing_shard_paren (SHARD user = 'u1', SHARD (host = '127.0.0.1', port = 9000)); -- { clientError SYNTAX_ERROR }

CREATE CLUSTER t05483_dup_cluster_user (
    user = 'a',
    user = 'b',
    SHARD (host = '127.0.0.1', port = 9000, user = 'c')
); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER t05483_dup_shard_host (
    SHARD (host = '127.0.0.1', host = '127.0.0.2', port = 9000)
); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER t05483_dup_replica_host (
    SHARD (
        REPLICA (host = '127.0.0.1', host = '127.0.0.2', port = 9000)
    )
); -- { serverError BAD_ARGUMENTS }
