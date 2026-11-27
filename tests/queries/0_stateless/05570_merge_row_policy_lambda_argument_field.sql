-- A row policy that reads an `ALIAS` column and a field of a lambda argument (`z.v`) must still
-- be applied at the read of a `Merge` child, before the user's `PREWHERE`: otherwise `PREWHERE`
-- runs on rows that the policy excludes and can show their values in an exception.

DROP TABLE IF EXISTS m_05570;
DROP TABLE IF EXISTS t_05570;

CREATE TABLE t_05570
(
    tenant String,
    payload String,
    arr Array(Tuple(v String)),
    t2 String ALIAS lower(tenant)
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05570 (tenant, payload, arr) VALUES ('a', '20', [tuple('a')]), ('B', 'secret', [tuple('a')]);

CREATE TABLE m_05570 (tenant String, payload String, arr Array(Tuple(v String)))
ENGINE = Merge(currentDatabase(), '^t_05570$');

CREATE ROW POLICY p_05570 ON t_05570 USING t2 = 'a' AND arrayExists(z -> z.v = 'a', arr) TO ALL;

SELECT count() FROM t_05570 PREWHERE toUInt64(payload) > 10;
SELECT count() FROM m_05570 PREWHERE toUInt64(payload) > 10;
SELECT tenant, payload FROM m_05570 ORDER BY tenant;

DROP ROW POLICY p_05570 ON t_05570;
DROP TABLE m_05570;
DROP TABLE t_05570;
