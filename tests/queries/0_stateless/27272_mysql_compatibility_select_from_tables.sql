/* create & populate table via ClickHouseQL */
SET sql_dialect='clickhouse';

DROP TABLE IF EXISTS d_mysql.t_mysql;
DROP DATABASE IF EXISTS d_mysql;

CREATE DATABASE IF NOT EXISTS d_mysql;
CREATE TABLE IF NOT EXISTS d_mysql.t_mysql
(
    user_id UInt32,
    message String,
    timestamp DateTime,
    metric Float32
)
ENGINE = MergeTree()
PRIMARY KEY (user_id, timestamp);

INSERT INTO d_mysql.t_mysql (user_id, message, timestamp, metric) VALUES
    (101, 'first row', toDateTime('2019-06-15 23:00:00'), -1.0), (102, 'second row', toDateTime('2020-06-15 23:00:00'), -1.41421), (102, 'third row', toDateTime('2021-06-15 23:00:00'), 2.718), (101, '4-th row', toDateTime('2022-06-15 23:00:00'), 3.14159);

/* mysql test queries */
SET sql_dialect='mysql';

SELECT "MOO: SHOW & DESCRIBE";
SHOW TABLES;
SHOW FULL TABLES;
SHOW COLUMNS FROM d_mysql.t_mysql;
DESC d_mysql.t_mysql;
DESCRIBE d_mysql.t_mysql;

SELECT "MOO: USE DATABASE";
USE system;
SELECT "MOO: ---";
SELECT user_id FROM d_mysql.t_mysql; -- explict db
SELECT "MOO: ---";
USE d_mysql;
SELECT user_id FROM t_mysql; -- implicit db
SELECT "MOO: ---";
SELECT user_id FROM d_mysql.t_mysql; -- excess

SELECT "MOO: SELECT columns";
SELECT "MOO: user_id";
SELECT user_id FROM t_mysql;
SELECT "MOO: message";
SELECT message FROM t_mysql;
SELECT "MOO: timestamp";
SELECT timestamp FROM t_mysql;
SELECT "MOO: metric";
SELECT metric FROM t_mysql;
SELECT "MOO: user_id, metric";
SELECT user_id, metric FROM t_mysql;
SELECT "MOO: *";
SELECT * FROM t_mysql;
SELECT "MOO: *, user_id, user_id";
SELECT *, user_id, user_id FROM t_mysql;

SELECT "MOO: SELECT columns + expressions";
SELECT "MOO: user_id * user_id";
SELECT user_id * user_id FROM t_mysql;
SELECT "MOO: user_id + metric";
SELECT user_id + metric FROM t_mysql;
SELECT "MOO: 1";
SELECT 1 FROM t_mysql;
SELECT "MOO: '1 one' alias";
SELECT 1 one FROM t_mysql;
SELECT "MOO: `1 AS one` alias";
SELECT 1 AS one FROM t_mysql;
SELECT "MOO: 1 one, user_id * user_id AS squared_user, one + squared_user";
SELECT 1 one, user_id * user_id AS squared_user, one + squared_user FROM t_mysql;

SELECT "MOO: WHERE";
SELECT "MOO: user_id = 101";
SELECT user_id FROM t_mysql WHERE user_id = 101;
SELECT "MOO: user_id = 102";
SELECT user_id FROM t_mysql WHERE user_id = 102;
SELECT "MOO: *, user_id > 0";
SELECT * FROM t_mysql WHERE user_id > 0;
SELECT "MOO: *, user_id < 0";
SELECT * FROM t_mysql WHERE user_id < 0;
SELECT "MOO: user_id * user_id > 10201";
SELECT user_id FROM t_mysql WHERE user_id * user_id > 10201;
SELECT "MOO: squared_user <= 10201";
SELECT user_id * user_id squared_user FROM t_mysql WHERE squared_user - 10200 = 1;
SELECT "MOO: user_id = 101 AND metric > 0";
SELECT user_id, metric FROM t_mysql WHERE user_id = 101 AND metric > 0;
SELECT "MOO: user_id = 101 AND (metric > 0 OR user_id = 102)";
SELECT user_id, metric FROM t_mysql WHERE user_id = 101 AND (metric > 0 OR user_id = 102);
SELECT "MOO: user_id = 101 AND metric > 0 OR user_id = 102";
SELECT user_id, metric FROM t_mysql WHERE user_id = 101 AND metric > 0 OR user_id = 102;
SELECT "MOO: (user_id = 101 AND metric > 0) OR user_id = 102";
SELECT user_id, metric FROM t_mysql WHERE (user_id = 101 AND metric > 0) OR user_id = 102;
SELECT "MOO: user_id = 101 XOR metric > 0";
SELECT user_id, metric FROM t_mysql WHERE user_id = 101 XOR metric > 0;

SELECT "MOO: ORDER BY";
SELECT "MOO: user_id ASC (implicit)";
SELECT user_id, metric FROM t_mysql ORDER BY user_id; --implicit ASC
SELECT "MOO: user_id ASC (explicit)";
SELECT user_id, metric FROM t_mysql ORDER BY user_id ASC;
SELECT "MOO: user_id DESC";
SELECT user_id, metric FROM t_mysql ORDER BY user_id DESC;
SELECT "MOO: user_id ASC (implicit), metric ASC (implicit)";
SELECT user_id, metric FROM t_mysql ORDER BY user_id, metric;
SELECT "MOO: user_id ASC (explicit), metric ASC (explicit)";
SELECT user_id, metric FROM t_mysql ORDER BY user_id ASC, metric ASC;
SELECT "MOO: user_id ASC (implicit), metric DESC";
SELECT user_id, metric FROM t_mysql ORDER BY user_id, metric DESC;
SELECT "MOO: user_id DESC, metric ASC (implicit)";
SELECT user_id, metric FROM t_mysql ORDER BY user_id DESC, metric;
SELECT "MOO: user_id DESC, metric ASC (explicit)";
SELECT user_id, metric FROM t_mysql ORDER BY user_id DESC, metric ASC;
SELECT "MOO: user_id DESC, metric DESC";
SELECT user_id, metric FROM t_mysql ORDER BY user_id DESC, metric DESC;


SET sql_dialect='clickhouse'; -- paranoid
