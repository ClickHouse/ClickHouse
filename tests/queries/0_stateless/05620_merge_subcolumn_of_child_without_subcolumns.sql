-- A Merge child whose engine cannot read subcolumns is asked for the whole column, and the Merge extracts the subcolumn.

DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS s;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS st;

CREATE TABLE mt (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO mt VALUES (1);
ALTER TABLE mt DETACH PARTITION tuple();

SELECT max(reason.null) <= 1 FROM merge('system', '^detached_parts$');
SELECT reason.null FROM merge('system', '^detached_parts$') WHERE database = currentDatabase() AND table = 'mt';
SELECT max(length(parts_postpone_reasons.keys)) >= 0 FROM merge('system', '^mutations$');

CREATE TABLE s (x Nullable(UInt8), a Array(UInt8), m Map(String, UInt8), t Tuple(p UInt8, q String), v Variant(String, UInt64)) ENGINE = StripeLog;
INSERT INTO s VALUES (1, [1], {'k':1}, (1,'a'), 'a'), (NULL, [1,2], {}, (2,'b'), 2), (3, [], {'x':2,'y':3}, (3,'c'), NULL);
SELECT x.null, a.size0, m.keys, t.p, v.String FROM merge(currentDatabase(), '^s$') ORDER BY ALL;
SELECT a.size0 FROM merge(currentDatabase(), '^s$') WHERE x.null = 0 ORDER BY ALL;

CREATE TABLE st (a Tuple(b UInt8), arr Array(String)) ENGINE = StripeLog;
INSERT INTO st VALUES ((11), ['x', 'y']);
CREATE TABLE m (`a.b` UInt8, arr Array(UInt8)) ENGINE = Merge(currentDatabase(), '^st$');
SELECT `a.b` FROM m;
SELECT arr.size0 FROM m;

DROP TABLE mt;
DROP TABLE s;
DROP TABLE m;
DROP TABLE st;
