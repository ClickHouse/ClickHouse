-- Tags: shard
-- A function written without parentheses is evaluated on the initiator, like its call with parentheses.

SELECT CURRENT_DATABASE = currentDatabase() FROM remote('127.0.0.2', system.one);
SELECT count() FROM remote('127.0.0.2', numbers(4)) WHERE SCHEMA = currentDatabase();
WITH DATABASE AS d SELECT d = currentDatabase() FROM remote('127.0.0.2', system.one);
WITH SCHEMA AS d SELECT arrayMap(x -> d, [1])[1] = currentDatabase() FROM remote('127.0.0.2', system.one);
SELECT CURRENT_DATABASE = currentDatabase() AS ok, count() FROM remote('127.0.0.{1,2}', numbers(4)) GROUP BY ok;

-- Inside ASOF JOIN ON neither form is constant-folded, so both are evaluated on the same server.
CREATE TABLE asof_l (k UInt8, t UInt8) ENGINE = MergeTree ORDER BY k;
CREATE TABLE asof_r (k UInt8, t UInt8, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO asof_l VALUES (1, 2);
INSERT INTO asof_r VALUES (1, 1, 'x');
SELECT count() FROM remote('127.0.0.2', currentDatabase(), asof_l) AS a ASOF JOIN asof_r AS b ON a.k = b.k AND a.t >= b.t AND CURRENT_DATABASE = currentDatabase();
WITH SCHEMA AS d SELECT (SELECT count() FROM remote('127.0.0.2', currentDatabase(), asof_l) AS a ASOF JOIN asof_r AS b ON a.k = b.k AND a.t >= b.t AND d = currentDatabase());
DROP TABLE asof_l;
DROP TABLE asof_r;
