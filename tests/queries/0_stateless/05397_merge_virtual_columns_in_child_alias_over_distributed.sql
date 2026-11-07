-- An `ALIAS` column of a table read through a `Distributed` child of a `Merge` table computes from
-- that table's own physical `_table`/`_database` column, not from the `Merge` virtual column of the
-- same name, which carries the matched child's name.

DROP TABLE IF EXISTS str_child;
DROP TABLE IF EXISTS str_dist;
DROP TABLE IF EXISTS str_m;

CREATE TABLE str_child (`_table` String, `_database` String, x UInt8, y String ALIAS concat(`_table`, '!'), z String ALIAS concat(`_database`, '?'))
ENGINE = MergeTree ORDER BY x;
INSERT INTO str_child VALUES ('physical', 'db', 1);
CREATE TABLE str_dist AS str_child ENGINE = Distributed(test_shard_localhost, currentDatabase(), str_child);
CREATE TABLE str_m (x UInt8, y String, z String) ENGINE = Merge(currentDatabase(), '^str_dist$');

SET prefer_localhost_replica = 0;

SELECT '-- _table';
SELECT _table, x, count() FROM str_m WHERE y = 'physical!' GROUP BY _table, x;
SELECT _table, x, count() FROM str_m WHERE y = 'str_dist!' GROUP BY _table, x;
SELECT _table, x, y FROM str_m;

SELECT '-- _database';
SELECT _database = currentDatabase(), x, count() FROM str_m WHERE z = 'db?' GROUP BY _database, x;
SELECT _database = currentDatabase(), x, count() FROM str_m WHERE z = concat(currentDatabase(), '?') GROUP BY _database, x;
SELECT _database = currentDatabase(), x, z FROM str_m;

DROP TABLE str_m;
DROP TABLE str_dist;
DROP TABLE str_child;
