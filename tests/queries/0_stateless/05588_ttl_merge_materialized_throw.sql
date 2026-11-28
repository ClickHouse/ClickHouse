-- Tags: no-replicated-database
-- A column TTL resets its column to the default, and a MATERIALIZED column computed from it is
-- recomputed inside the TTL merge. A default that throws on the reset value (intDiv(100, x) with
-- x reset to 0) used to fail the whole merge, which retried forever and wedged the table. The
-- recompute failure is isolated now: the merge proceeds and the column keeps its old values.

DROP TABLE IF EXISTS ttl_mat_throw_05588;
DROP TABLE IF EXISTS ttl_mat_ok_05588;

CREATE TABLE ttl_mat_throw_05588
(
    d DateTime,
    x Int32 TTL d + INTERVAL 1 SECOND,
    m Int32 MATERIALIZED intDiv(100, x)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO ttl_mat_throw_05588 (d, x) VALUES (now() - INTERVAL 1 HOUR, 5);
INSERT INTO ttl_mat_throw_05588 (d, x) VALUES (now() - INTERVAL 1 HOUR, 7);

OPTIMIZE TABLE ttl_mat_throw_05588 FINAL;

-- The TTL is applied (x is reset), the throw is only logged, and m keeps its old values.
SELECT x, m FROM ttl_mat_throw_05588 ORDER BY x;

-- The control with a non-throwing default recomputes as usual.
CREATE TABLE ttl_mat_ok_05588
(
    d DateTime,
    x Int32 TTL d + INTERVAL 1 SECOND,
    m Int32 MATERIALIZED x + 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO ttl_mat_ok_05588 (d, x) VALUES (now() - INTERVAL 1 HOUR, 5);
INSERT INTO ttl_mat_ok_05588 (d, x) VALUES (now() - INTERVAL 1 HOUR, 7);

OPTIMIZE TABLE ttl_mat_ok_05588 FINAL;
SELECT x, m FROM ttl_mat_ok_05588 ORDER BY x;

DROP TABLE ttl_mat_throw_05588;
DROP TABLE ttl_mat_ok_05588;
