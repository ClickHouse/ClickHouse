SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;

-- A full-definition ATTACH rejects an ALIAS column that reads itself, like CREATE.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_direct (x UInt8, c0 UInt8 ALIAS c0) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_self (x UInt8, c0 Nullable(IPv6) ALIAS c0.null) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
SELECT c5 FROM format(CSV, 'c0 Int32, c5 String ALIAS c5.size', '1'); -- { serverError CYCLIC_ALIASES }
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mutual (x UInt8, c0 Nullable(UInt8) ALIAS c1.null, c1 Nullable(UInt8) ALIAS c0) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }

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

-- A cycle through a branch of `if` that is not taken.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_dead (x Int32, a Int32 ALIAS if(0, b, 1), b Int32 ALIAS arraySum(arrayMap(z -> z + a, [2])), c Int32 ALIAS if(0, d, 1), d Int32 ALIAS q + ((x + c) AS q)) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
