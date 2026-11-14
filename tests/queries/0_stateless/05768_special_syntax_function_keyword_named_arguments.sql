-- A function call or an identifier named `in`, `for` or `placing` inside position/substring/overlay
-- must not be read as the infix keyword of these functions.

SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT position(`in`, ''y''), position(-`in`, ''y''), position(`in`.x, ''y''), position(`in`[1], ''y''), position(x IN `in`)'));
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT position(`in`(a, (b, c)), ''y''), position(`not`(`in`(a, (b, c))), ''y'', z)'));
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT substring(s, `for`), substring(s, `for`, 2), substring(s FROM `for`(1))'));
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT overlay(`placing`, ''a'', 1), overlayUTF8(`placing`(s), ''a'', 1)'));

SELECT formatQuerySingleLine('SELECT position(in(a, b), ''y''), position(in IN x), overlay(s PLACING placing FROM 1), substring(from, 1)');
SELECT formatQuerySingleLine('SELECT overlay(s PLACING from FROM 1)');
SELECT formatQuerySingleLine('SELECT overlay(s PLACING r FROM for)');

SELECT position('abc' IN 'xabc'), substring('abcdef' FROM 2 FOR 3), overlay('abcdef' PLACING 'XY' FROM 2 FOR 3);

CREATE TABLE t (`in` String, `for` UInt8, `placing` String) ENGINE = Memory;
INSERT INTO t VALUES ('abc', 2, 'xyz');
CREATE VIEW v AS SELECT position(`in`, 'b') AS p, substring('abcdef', `for`) AS s, overlay(`placing`, 'Q', 2) AS o FROM t;
DETACH TABLE v;
ATTACH TABLE v;
SELECT * FROM v;
