-- Tags: log-engine

-- A full-definition ATTACH refuses a column of dynamic structure for an engine that does not support it, like CREATE does.

SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt64) ENGINE = Log;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1), (2);
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t;

ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id Dynamic(max_types = 5)) ENGINE = Log; -- { serverError ILLEGAL_COLUMN }
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_tiny (d Dynamic) ENGINE = TinyLog; -- { serverError ILLEGAL_COLUMN }
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_stripe (d Dynamic) ENGINE = StripeLog; -- { serverError ILLEGAL_COLUMN }
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_json (j JSON) ENGINE = Log; -- { serverError ILLEGAL_COLUMN }

-- The refused ATTACH left the data and the stored definition as they were.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (3);
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t ORDER BY id;

-- A full-definition ATTACH is still allowed where the engine supports the column.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mt (d Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_mt VALUES (1), ('a');
SELECT d, dynamicType(d) FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_mt ORDER BY toString(d);

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
