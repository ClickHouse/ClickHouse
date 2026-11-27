-- Tags: no-fasttest
-- no-fasttest: the fast test does not build the encryption functions

-- An expression copied from the source can hold credentials, which `SHOW CREATE TABLE` masks, so copying it needs `SELECT` on the source.

DROP USER IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
CREATE USER {CLICKHOUSE_DATABASE:Identifier};
GRANT CREATE TABLE, SHOW COLUMNS ON * TO {CLICKHOUSE_DATABASE:Identifier};
GRANT TABLE ENGINE ON MergeTree TO {CLICKHOUSE_DATABASE:Identifier};

CREATE TABLE default_src (s String, e String DEFAULT encrypt('aes-128-ecb', s, '0123456789abcdef')) ENGINE = MergeTree ORDER BY s;
CREATE TABLE materialized_src (s String, e String MATERIALIZED encrypt('aes-128-ecb', s, '0123456789abcdef')) ENGINE = MergeTree ORDER BY s;
CREATE TABLE alias_src (s String, e String ALIAS encrypt('aes-128-ecb', s, '0123456789abcdef')) ENGINE = MergeTree ORDER BY s;
CREATE TABLE ttl_src (s String, d DateTime, e String TTL d + toIntervalSecond(length(encrypt('aes-128-ecb', s, '0123456789abcdef')))) ENGINE = MergeTree ORDER BY s;
CREATE TABLE index_src (s String, INDEX i encrypt('aes-128-ecb', s, '0123456789abcdef') TYPE minmax) ENGINE = MergeTree ORDER BY s;
CREATE TABLE projection_src (s String, PROJECTION p (SELECT s, any(encrypt('aes-128-ecb', s, '0123456789abcdef')) GROUP BY s)) ENGINE = MergeTree ORDER BY s;
CREATE TABLE constraint_src (s String, CONSTRAINT c CHECK encrypt('aes-128-ecb', s, '0123456789abcdef') != '') ENGINE = MergeTree ORDER BY s;
CREATE TABLE order_by_src (s String) ENGINE = MergeTree ORDER BY encrypt('aes-128-ecb', s, '0123456789abcdef');
CREATE TABLE partition_by_src (s String) ENGINE = MergeTree PARTITION BY encrypt('aes-128-ecb', s, '0123456789abcdef') ORDER BY s;
CREATE TABLE aliased_src (s String, e String DEFAULT encrypt('aes-128-ecb', s, '0123456789abcdef')) ENGINE = MergeTree ORDER BY s;
CREATE TABLE alias_engine_src ENGINE = Alias('aliased_src');

EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE default_copy AS default_src; -- { serverError ACCESS_DENIED }
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE materialized_copy AS materialized_src; -- { serverError ACCESS_DENIED }
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE alias_copy AS alias_src; -- { serverError ACCESS_DENIED }
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE ttl_copy AS ttl_src; -- { serverError ACCESS_DENIED }
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE index_copy AS index_src; -- { serverError ACCESS_DENIED }
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE projection_copy AS projection_src; -- { serverError ACCESS_DENIED }
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE constraint_copy AS constraint_src; -- { serverError ACCESS_DENIED }
-- an explicit engine inherits no storage definition, but the keys are still copied
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE order_by_copy AS order_by_src ENGINE = MergeTree; -- { serverError ACCESS_DENIED }
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE partition_by_copy AS partition_by_src ENGINE = MergeTree; -- { serverError ACCESS_DENIED }
-- an `Alias` gives the metadata of its target, so the copy needs `SELECT` on the target
GRANT SELECT ON alias_engine_src TO {CLICKHOUSE_DATABASE:Identifier};
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE alias_engine_copy AS alias_engine_src ENGINE = MergeTree; -- { serverError ACCESS_DENIED }

GRANT SELECT ON * TO {CLICKHOUSE_DATABASE:Identifier};
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE default_copy AS default_src;
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE materialized_copy AS materialized_src;
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE alias_copy AS alias_src;
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE ttl_copy AS ttl_src;
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE index_copy AS index_src;
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE projection_copy AS projection_src;
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE constraint_copy AS constraint_src;
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE order_by_copy AS order_by_src ENGINE = MergeTree;
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE partition_by_copy AS partition_by_src ENGINE = MergeTree;
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} CREATE TABLE alias_engine_copy AS alias_engine_src ENGINE = MergeTree;

DROP USER {CLICKHOUSE_DATABASE:Identifier};
