-- INNER ANY that selects no right column runs on a key-only hash table, and its result must not depend on that.

CREATE TABLE l (k UInt64, n Nullable(UInt64), s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 7;
CREATE TABLE r (k UInt64, n Nullable(UInt64), s String, p String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 7;

INSERT INTO l SELECT number % 20, if(number % 4 = 0, NULL, number % 20), toString(number % 20) FROM numbers(100);
INSERT INTO r SELECT number * 3 % 30, if(number % 3 = 0, NULL, number * 3 % 30), toString(number * 3 % 30), 'r' FROM numbers(40);

SET max_threads = 4, max_block_size = 9;

SELECT 'key only';
SELECT k FROM l INNER ANY JOIN r USING (k) ORDER BY k;
SELECT k FROM l INNER ANY JOIN r USING (k) ORDER BY k SETTINGS enable_join_key_only_hash_tables = 0;

SELECT 'right column selected';
SELECT k, p FROM l INNER ANY JOIN r USING (k) ORDER BY k;

SELECT 'nullable key';
SELECT n FROM l INNER ANY JOIN r USING (n) ORDER BY n;

SELECT 'composite key';
SELECT k, s FROM l INNER ANY JOIN r USING (k, s) ORDER BY k;

SELECT 'several disjuncts';
SELECT count() FROM l INNER ANY JOIN r ON l.k = r.k OR l.s = r.p;
