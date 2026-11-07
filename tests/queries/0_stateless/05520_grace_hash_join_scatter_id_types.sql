-- Grace hash join splits both sides into buckets with `scatterBlock`. The bucket ids are `UInt16` when
-- `grace_hash_join_max_buckets` is at most 65536 (the default is 1024), and `UInt64` above it. The setting
-- picks the type, not the number of buckets the join reaches. Both types must return the same rows as a
-- hash join that does not spill.

DROP TABLE IF EXISTS grace_ids_left;
DROP TABLE IF EXISTS grace_ids_right;

CREATE TABLE grace_ids_left (k UInt64, s String, a Array(UInt64)) ENGINE = Memory;
CREATE TABLE grace_ids_right (k UInt64, x UInt64, n Nullable(String)) ENGINE = Memory;

-- Every key has 3 rows on the left and 2 on the right, so the join has 6 rows per key.
INSERT INTO grace_ids_left SELECT number % 100000, toString(number), range(number % 4) FROM numbers(300000);
INSERT INTO grace_ids_right SELECT number % 100000, number, if(number % 3 = 0, NULL, toString(number)) FROM numbers(200000);

SET join_algorithm = 'grace_hash';
SET grace_hash_join_initial_buckets = 8;
-- The limit is small compared with one of the 8 buckets of the right side, so the join also splits the
-- bucket it holds in memory.
SET max_bytes_before_external_join = '1Mi', max_bytes_ratio_before_external_join = 0;

SELECT 'UInt16 ids', count(), sum(l.k), sum(r.x), count(r.n), sum(length(l.s)), sum(length(l.a))
FROM grace_ids_left AS l INNER JOIN grace_ids_right AS r ON l.k = r.k;

SELECT 'UInt64 ids', count(), sum(l.k), sum(r.x), count(r.n), sum(length(l.s)), sum(length(l.a))
FROM grace_ids_left AS l INNER JOIN grace_ids_right AS r ON l.k = r.k
SETTINGS grace_hash_join_max_buckets = 100000;

-- The sums above can miss a row joined to the wrong partner. A sum of the hash of each joined row catches it.
WITH
    (
        SELECT sum(cityHash64(l.k, l.s, l.a, r.x, ifNull(r.n, '')))
        FROM grace_ids_left AS l INNER JOIN grace_ids_right AS r ON l.k = r.k
    ) AS uint16_ids,
    (
        SELECT sum(cityHash64(l.k, l.s, l.a, r.x, ifNull(r.n, '')))
        FROM grace_ids_left AS l INNER JOIN grace_ids_right AS r ON l.k = r.k
        SETTINGS grace_hash_join_max_buckets = 100000
    ) AS uint64_ids,
    (
        SELECT sum(cityHash64(l.k, l.s, l.a, r.x, ifNull(r.n, '')))
        FROM grace_ids_left AS l INNER JOIN grace_ids_right AS r ON l.k = r.k
        SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0
    ) AS hash_join
SELECT 'same rows as hash join', uint16_ids = hash_join, uint64_ids = hash_join;

DROP TABLE grace_ids_left;
DROP TABLE grace_ids_right;
