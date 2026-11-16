-- Keywords used as identifiers must stay quoted to re-parse to same AST

SELECT formatQuerySingleLine('SELECT `not`, b FROM t');
SELECT formatQuerySingleLine('SELECT `not` + 1 AS x FROM t WHERE `not` = 1 ORDER BY `not`');
SELECT formatQuerySingleLine('SELECT x[`not`], f(`not`, 1), `not`.y');

SELECT formatQuerySingleLine('SELECT `not`(1, 2)') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('SELECT `not`(1) OVER ()') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('SELECT `columns`(a, b)') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('SELECT `as`::Int32') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('SELECT `interval` DAY') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('SELECT * EXCEPT (`strict`)') AS f, formatQuerySingleLine(f) = f;

DROP TABLE IF EXISTS not_identifier_t;
DROP VIEW IF EXISTS not_identifier_v;

CREATE TABLE not_identifier_t (`not` UInt8) ENGINE = MergeTree ORDER BY `not`;
INSERT INTO not_identifier_t VALUES (5);

CREATE VIEW not_identifier_v AS SELECT `not` + 1 AS x FROM not_identifier_t;
DETACH TABLE not_identifier_v;
ATTACH TABLE not_identifier_v;
SELECT * FROM not_identifier_v;

DETACH TABLE not_identifier_t;
ATTACH TABLE not_identifier_t;
SELECT * FROM not_identifier_t;

DROP VIEW not_identifier_v;
DROP TABLE not_identifier_t;
