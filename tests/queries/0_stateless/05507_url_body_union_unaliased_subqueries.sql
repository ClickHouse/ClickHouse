-- Tags: no-fasttest
-- no-fasttest: the url table function is not in the fast test build

-- The subquery in `body(...)` is planned on its own, and it has to get unique table aliases like any other query:
-- the branches of a `UNION` share the column identifiers, so two subqueries without aliases producing `q` used to clash.
SET enable_analyzer = 1;

SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8',
    body((SELECT q FROM (SELECT 'SELECT 1 FORMAT TSV' AS q) UNION DISTINCT SELECT q FROM (SELECT 'SELECT 1 FORMAT TSV' AS q)), 'TSVRaw'));

SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8',
    body(SELECT q FROM (SELECT 'SELECT 2 FORMAT TSV' AS q) WHERE q IN (SELECT q FROM (SELECT 'SELECT 2 FORMAT TSV' AS q)) FORMAT TSVRaw));
