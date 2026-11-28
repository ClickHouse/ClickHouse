SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;

-- ATTACH does not validate the definition, so the cycle is found when the column is read.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_direct (x UInt8, c0 UInt8 ALIAS c0) ENGINE = Memory;
SELECT c0 FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_direct; -- { serverError CYCLIC_ALIASES }

ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_self (x UInt8, c0 Nullable(IPv6) ALIAS c0.null) ENGINE = Memory;
SELECT c0 FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_self; -- { serverError CYCLIC_ALIASES }
SELECT c0 FROM remote('127.0.0.1', {CLICKHOUSE_DATABASE_1:Identifier}, t_self); -- { serverError CYCLIC_ALIASES }
SELECT if(0, c0, NULL), c0 FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_self; -- { serverError CYCLIC_ALIASES }
SELECT multiIf(0, c0, NULL), c0 FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_self; -- { serverError CYCLIC_ALIASES }
SELECT c5 FROM format(CSV, 'c0 Int32, c5 String ALIAS c5.size', '1'); -- { serverError CYCLIC_ALIASES }
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.r (x UInt8, k Nullable(IPv6)) ENGINE = Memory;
SELECT x, c0 AS k FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_self JOIN {CLICKHOUSE_DATABASE_1:Identifier}.r USING (x, k) SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1; -- { serverError CYCLIC_ALIASES }
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.m (x UInt8, c0 Nullable(IPv6)) ENGINE = Merge({CLICKHOUSE_DATABASE_1:Identifier}, '^t_self$');
SELECT c0 FROM {CLICKHOUSE_DATABASE_1:Identifier}.m; -- { serverError CYCLIC_ALIASES }

ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mutual (x UInt8, c0 Nullable(UInt8) ALIAS c1.null, c1 Nullable(UInt8) ALIAS c0) ENGINE = Memory;
SELECT c0 FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_mutual; -- { serverError CYCLIC_ALIASES }

-- Not cycles.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_ok (x UInt8, arr Array(UInt8), a Tuple(n Nullable(UInt8)) ALIAS tuple(x), b Nullable(UInt8) ALIAS a.n, c0 Array(UInt8) ALIAS arrayMap(c0 -> c0 * 2, arr)) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_ok (x, arr) VALUES (3, [1, 2]);
SELECT a, b, c0 FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_ok;

-- `t_probe.x` and `n` first probe the columns `t_probe` and `n.a`, whose types do not match.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_probe (x UInt8, a UInt8 ALIAS t_probe.x, t_probe UInt8 ALIAS a, `n.a` UInt64 ALIAS length(n), `n.b` Array(UInt8)) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_probe (x, `n.b`) VALUES (7, [1, 2]);
SELECT t_probe FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_probe;
SELECT a FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_probe;
SELECT `n.a` FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_probe;

-- The cycles go through a branch of `if` that is not taken: `b` and `d` are resolved again after the error in that branch.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_dead (x Int32, a Int32 ALIAS if(0, b, 1), b Int32 ALIAS arraySum(arrayMap(z -> z + a, [2])), c Int32 ALIAS if(0, d, 1), d Int32 ALIAS q + ((x + c) AS q)) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_dead (x) VALUES (7);
SELECT a, b FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_dead;
SELECT c, d FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_dead;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
