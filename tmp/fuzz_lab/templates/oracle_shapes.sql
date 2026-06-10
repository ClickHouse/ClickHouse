-- Oracle-shaped seed queries. Each query is crafted to satisfy the strict
-- preconditions of one oracle family in QueryOracleChecker, so that fuzzed
-- mutants of it keep exercising that oracle (oracle_mode preserves topmost
-- WHERE / GROUP BY / HAVING). Tables _s1.._s4 come from populate.sql.
--
-- Constraints respected throughout: no LIMIT, no PREWHERE, no ORDER BY
-- (except where noted), no arrayJoin in SELECT/WHERE, no window functions,
-- no non-deterministic functions, no float-sensitive aggregates
-- (sum/avg/stddev/var/covar/corr/skew/kurt), no combinator suffixes.

-- ============================================================
-- A. TLP GROUP BY: every SELECT expr tree-hash-equals a GROUP BY expr,
--    WHERE present, no aggregates, no HAVING.
-- ============================================================
SELECT g FROM _s1 WHERE b > 100 GROUP BY g;
SELECT g, c FROM _s1 WHERE a < 300 GROUP BY g, c;
SELECT a % 10 FROM _s1 WHERE d > 0 GROUP BY a % 10;
SELECT a % 10, length(c) FROM _s1 WHERE b IS NOT NULL GROUP BY a % 10, length(c);
SELECT h FROM _s1 WHERE e IS NULL GROUP BY h;
SELECT b FROM _s1 WHERE g = 1 GROUP BY b;
SELECT y FROM _s2 WHERE z > 50 GROUP BY y;
SELECT y, z FROM _s2 WHERE x < 200 GROUP BY y, z;
SELECT z IS NULL FROM _s2 WHERE x % 2 = 0 GROUP BY z IS NULL;
SELECT id % 7 FROM _s3 WHERE val > 100 GROUP BY id % 7;
SELECT en FROM _s4 WHERE dec > 10 GROUP BY en;
SELECT en, lc_n FROM _s4 WHERE id < 100 GROUP BY en, lc_n;
SELECT tup.1 FROM _s4 WHERE has(arr, 2) GROUP BY tup.1;
SELECT length(arr) FROM _s4 WHERE ip > toIPv4('10.0.0.100') GROUP BY length(arr);
SELECT toMonth(f) FROM _s1 WHERE c != '3' GROUP BY toMonth(f);

-- ============================================================
-- B. TLP Aggregate: SELECT list is only safe aggregates + bare GROUP BY
--    exprs; WHERE present; no HAVING; no combinators; no float-sensitive
--    aggregates. Safe: count, min, max, uniqExact, groupBitAnd/Or/Xor.
-- ============================================================
SELECT count() FROM _s1 WHERE b > 500;
SELECT count(), g FROM _s1 WHERE d < 10 GROUP BY g;
SELECT min(a), max(a), g FROM _s1 WHERE b IS NOT NULL GROUP BY g;
SELECT count(b) FROM _s1 WHERE e IS NOT NULL;
SELECT uniqExact(c), g FROM _s1 WHERE a > 50 GROUP BY g;
SELECT min(b), max(h) FROM _s1 WHERE g < 2;
SELECT count(), y FROM _s2 WHERE z IS NULL GROUP BY y;
SELECT max(x), min(z), y FROM _s2 WHERE x != 17 GROUP BY y;
SELECT uniqExact(z) FROM _s2 WHERE y IN ('1', '2', '3');
SELECT count(), min(val) FROM _s3 WHERE id % 3 = 1;
SELECT max(val), id % 5 FROM _s3 WHERE val < 1000 GROUP BY id % 5;
SELECT groupBitOr(g) FROM _s1 WHERE a < 400;
SELECT groupBitXor(x), y FROM _s2 WHERE z >= 0 GROUP BY y;
SELECT count(), en FROM _s4 WHERE dt > toDateTime('2023-01-02 00:00:00') GROUP BY en;
SELECT min(dec), max(dec), en FROM _s4 WHERE length(arr) > 1 GROUP BY en;
SELECT uniqExact(uid), tup.1 FROM _s4 WHERE id % 2 = 0 GROUP BY tup.1;
SELECT count(lc_n) FROM _s4 WHERE mp['k0'] != '5';

-- ============================================================
-- C. TLP HAVING: GROUP BY + HAVING (aggregates allowed), no LIMIT /
--    PREWHERE / DISTINCT; WHERE optional but included for TLP WHERE combo.
-- ============================================================
SELECT g, count() FROM _s1 GROUP BY g HAVING count() > 5;
SELECT g, min(b) FROM _s1 WHERE a < 450 GROUP BY g HAVING min(b) IS NOT NULL;
SELECT c, max(d) FROM _s1 GROUP BY c HAVING max(d) > -20;
SELECT h, count() FROM _s1 GROUP BY h HAVING h IS NOT NULL;
SELECT y, uniqExact(x) FROM _s2 GROUP BY y HAVING uniqExact(x) >= 10;
SELECT y, count(z) FROM _s2 WHERE x > 5 GROUP BY y HAVING count(z) < 100;
SELECT id % 4, max(val) FROM _s3 GROUP BY id % 4 HAVING max(val) % 2 = 0;
SELECT en, count() FROM _s4 GROUP BY en HAVING min(id) < 50;
SELECT lc_n, max(dec) FROM _s4 WHERE id != 13 GROUP BY lc_n HAVING max(dec) > 1;

-- ============================================================
-- D. TLP DISTINCT: DISTINCT + WHERE, no aggregates / GROUP BY / LIMIT.
-- ============================================================
SELECT DISTINCT g FROM _s1 WHERE b > 0;
SELECT DISTINCT c, g FROM _s1 WHERE d != 0;
SELECT DISTINCT h FROM _s1 WHERE a % 2 = 1;
SELECT DISTINCT e IS NULL FROM _s1 WHERE f > toDate('2020-03-01');
SELECT DISTINCT y FROM _s2 WHERE z < 100;
SELECT DISTINCT z FROM _s2 WHERE y != '4';
SELECT DISTINCT val % 10 FROM _s3 WHERE id > 20;
SELECT DISTINCT en FROM _s4 WHERE dt64 < toDateTime64('2023-01-01 00:02:00.000', 3);
SELECT DISTINCT lc_n, en FROM _s4 WHERE dec < 60;
SELECT DISTINCT tup FROM _s4 WHERE id % 3 != 0;

-- ============================================================
-- E. TLP WHERE / NoREC / Identity WHERE / DQP: rich WHERE predicates,
--    especially over Nullable columns (three-valued logic is where TLP
--    partitioning p / NOT p / p IS NULL finds bugs), plus JOINs and
--    IN / EXISTS subqueries. No GROUP BY / aggregates / LIMIT.
-- ============================================================
SELECT a, b FROM _s1 WHERE b > 1000;
SELECT a, b, e FROM _s1 WHERE e LIKE '%3%';
SELECT a FROM _s1 WHERE b + h > 100;
SELECT a, d FROM _s1 WHERE (b > 100) OR (h < 50);
SELECT a, c FROM _s1 WHERE (b > 100) AND (e IS NULL);
SELECT a FROM _s1 WHERE NOT (b = 30);
SELECT a, h FROM _s1 WHERE h = b;
SELECT x, y FROM _s2 WHERE z = x % 200;
SELECT x FROM _s2 WHERE if(z IS NULL, x > 150, z > 100);
SELECT a, c FROM _s1 WHERE c IN (SELECT y FROM _s2 WHERE z > 10);
SELECT x FROM _s2 WHERE x IN (SELECT id FROM _s3 WHERE val % 3 = 0);
SELECT a FROM _s1 WHERE exists(SELECT 1 FROM _s3 WHERE _s3.id = _s1.a AND _s3.val > 50);
SELECT a FROM _s1 WHERE a NOT IN (SELECT x FROM _s2 WHERE y = '1');
SELECT s1.a, s2.y FROM _s1 AS s1 INNER JOIN _s2 AS s2 ON s1.a = s2.x WHERE s1.b > s2.z;
SELECT s1.a, s2.z FROM _s1 AS s1 LEFT JOIN _s2 AS s2 ON s1.a = s2.x WHERE s2.z IS NULL;
SELECT s2.x, s3.val FROM _s2 AS s2 FULL JOIN _s3 AS s3 ON s2.x = s3.id WHERE s3.val > 70 OR s2.z < 5;
SELECT s1.a FROM _s1 AS s1 RIGHT JOIN _s3 AS s3 ON s1.a = s3.id WHERE s1.b IS NOT NULL;
SELECT id, arr FROM _s4 WHERE has(arr_n, NULL);
SELECT id FROM _s4 WHERE mp['k1'] > mp['k0'];
SELECT id, fs FROM _s4 WHERE fs < toFixedString('500', 8);
SELECT id FROM _s4 WHERE ip BETWEEN toIPv4('10.0.0.10') AND toIPv4('10.0.0.200');
SELECT id, dec FROM _s4 WHERE dec * 2 > 50;
SELECT id FROM _s4 WHERE tup = (5, '5');
SELECT id FROM _s4 WHERE arrayExists(v -> v IS NULL, arr_n);
SELECT a, f FROM _s1 WHERE f - INTERVAL 10 DAY > toDate('2020-06-01');
SELECT a FROM _s1 WHERE multiIf(g = 0, b > 0, g = 1, b < 0, b IS NULL);
SELECT a, e FROM _s1 WHERE coalesce(e, c) = '3';
SELECT a FROM _s1 WHERE assumeNotNull(h) > 200 AND h IS NOT NULL;
SELECT x, y FROM _s2 WHERE position(y, '3') > 0;

-- ============================================================
-- F. Subquery wrap / DQP broader shapes (ORDER BY allowed for wrap;
--    GROUP BY + aggregates fine for DQP and Identity WHERE).
-- ============================================================
SELECT g, count() AS cnt FROM _s1 WHERE b < 3000 GROUP BY g HAVING cnt > 1;
SELECT y, min(x), max(x) FROM _s2 WHERE z IS NOT NULL GROUP BY y;
SELECT en, uniqExact(id) FROM _s4 WHERE dec != 7 GROUP BY en;
SELECT DISTINCT g, h FROM _s1 WHERE b * 2 > a;
SELECT a, b FROM _s1 WHERE b > 100 ORDER BY a;
SELECT y, count() FROM _s2 WHERE x < 250 GROUP BY y ORDER BY y;
SELECT s1.g, count() FROM _s1 AS s1 INNER JOIN _s2 AS s2 ON s1.a = s2.x WHERE s2.z > 3 GROUP BY s1.g;
SELECT id % 6, min(val), max(val) FROM _s3 WHERE val != 49 GROUP BY id % 6;

-- ============================================================
-- G. Rich-schema queries (tables from populate_rich.sql). These exercise
--    skip indexes, aggregating/replacing engines + FINAL, the storage-Join
--    engine, and projections — the bug-dense paths absent from _s1.._s4.
--    Every fuzzed mutant keeps hitting these code paths.
-- ============================================================
-- minmax skip index over floats incl. NaN/Inf (TLP WHERE / NoREC / Identity)
SELECT id, f FROM _r_minmax WHERE f > 0;
SELECT id FROM _r_minmax WHERE NOT (f >= 0 AND f <= 10);
SELECT id, fn FROM _r_minmax WHERE fn < 5;
SELECT id FROM _r_minmax WHERE i BETWEEN -50 AND 50;
SELECT id FROM _r_minmax WHERE f != f;
-- set / bloom / tokenbf / ngrambf string indexes
SELECT id, s FROM _r_skip_str WHERE s = 'tok3 word2';
SELECT id FROM _r_skip_str WHERE s != 'tok0 word0';
SELECT id FROM _r_skip_str WHERE hasToken(s, 'word3');
SELECT id, sn FROM _r_skip_str WHERE sn IS NULL;
SELECT id FROM _r_skip_str WHERE has(arr, 3);
SELECT id, fs FROM _r_skip_str WHERE fs = toFixedString('5', 8);
-- map bloom filter (mapKeys / mapValues)
SELECT id FROM _r_map WHERE m['k1'] = 'v1';
SELECT id, m FROM _r_map WHERE m[''] = '';
SELECT id FROM _r_map WHERE mi['a'] > 10;
SELECT id FROM _r_map WHERE NOT (m['k0'] = 'v0');
-- text index, Nullable column, all the search functions
SELECT id FROM _r_text WHERE hasToken(t, 'hello');
SELECT id FROM _r_text WHERE NOT hasToken(t, 'world');
SELECT id, t FROM _r_text WHERE hasToken(t, 'foo') OR t IS NULL;
-- aggregating / replacing / summing / collapsing engines, with and w/o FINAL
SELECT key, mx, mn FROM _r_agg WHERE mx > 10;
SELECT key, max(mx) FROM _r_agg GROUP BY key HAVING max(mx) > 5;
SELECT key, v, val FROM _r_repl WHERE v = 2;
SELECT key, val FROM _r_repl WHERE val IS NOT NULL;
SELECT key, s FROM _r_sum WHERE s > 5;
SELECT key FROM _r_collapse WHERE v > 3;
-- storage Join engine (RIGHT JOIN, the bug-#7 surface)
SELECT l.k1, j.a FROM _r_left AS l ALL RIGHT JOIN _r_join AS j USING (k1, k2) WHERE j.a > 10;
SELECT l.val, j.b FROM _r_left AS l ALL RIGHT JOIN _r_join AS j ON l.k1 = j.k1 AND l.k2 = j.k2 WHERE j.b IS NULL;
SELECT j.a, j.b FROM _r_left AS l ALL RIGHT JOIN _r_join AS j USING (k1, k2) WHERE j.a < 100;
-- projection table
SELECT g, v FROM _r_proj WHERE v > 0;
SELECT g, sum(v) FROM _r_proj WHERE id < 80 GROUP BY g;
SELECT id FROM _r_proj WHERE g = 3;
