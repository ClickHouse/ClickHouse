SET enable_analyzer = 1;

-- A qualifier that matches a table expression without such a column must not stop the resolution:
-- the other JOIN side is tried as well.

-- https://github.com/ClickHouse/ClickHouse/issues/134214
-- The subquery alias `one` collides with the name of the table `system.one`.
SELECT one.x FROM (SELECT 1 AS x) AS one INNER JOIN system.one AS o ON o.dummy + 1 = one.x;
SELECT one.x FROM (SELECT 1 AS x) AS one, system.one AS o;
SELECT one.x FROM system.one AS o INNER JOIN (SELECT 1 AS x) AS one ON o.dummy + 1 = one.x;
SELECT one.x FROM (SELECT 1 AS x) AS one LEFT SEMI JOIN system.one AS o ON o.dummy + 1 = one.x;
SELECT one.x, t.y FROM (SELECT 1 AS x) AS one INNER JOIN system.one AS o ON o.dummy + 1 = one.x INNER JOIN (SELECT 1 AS y) AS t ON t.y = one.x;
SELECT one.dummy, one.x FROM (SELECT 1 AS x) AS one INNER JOIN system.one AS o ON o.dummy + 1 = x;

-- https://github.com/ClickHouse/ClickHouse/issues/132492
-- Two tables with the same name in different databases.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.users (uid Int16, name String, age Int16) ENGINE = MergeTree ORDER BY uid;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.users (uid1 Int16, name String, age Int16) ENGINE = MergeTree ORDER BY uid1;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.users VALUES (1, 'Alice', 30), (2, 'Bob', 40), (3, 'Carol', 50);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.users VALUES (1, 'Dave', 31), (2, 'Eve', 41), (4, 'Frank', 61);

SELECT users.uid, users.uid1
FROM {CLICKHOUSE_DATABASE:Identifier}.users
JOIN {CLICKHOUSE_DATABASE_1:Identifier}.users ON users.uid = users.uid1
ORDER BY users.uid;

SELECT users.uid1, users.uid
FROM {CLICKHOUSE_DATABASE_1:Identifier}.users
JOIN {CLICKHOUSE_DATABASE:Identifier}.users ON users.uid1 = users.uid
ORDER BY users.uid1;

-- A column that no candidate has is still an error.
SELECT users.nope
FROM {CLICKHOUSE_DATABASE:Identifier}.users
JOIN {CLICKHOUSE_DATABASE_1:Identifier}.users ON users.uid = users.uid1; -- { serverError UNKNOWN_IDENTIFIER }

SELECT one.x FROM system.one AS o; -- { serverError UNKNOWN_IDENTIFIER }
SELECT one.nope FROM (SELECT 1 AS x) AS one INNER JOIN system.one AS o ON 1; -- { serverError UNKNOWN_IDENTIFIER }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.users;
