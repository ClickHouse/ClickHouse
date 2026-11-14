-- Tags: no-fasttest
-- Tag no-fasttest: the `mysql` table function exists only in a build with MySQL support.

-- The address argument of an external-database table function is a brace expression:
-- `{N..M}` is a numeric interval and `{a|b}` a list of replicas, as documented in
-- docs/reference/functions/table-functions/remote.mdx, section "Globbing".
-- The expander rejects four malformed forms, each with its own message in
-- src/Common/parseRemoteDescription.cpp (RemoteDescriptionGenerator): an unclosed brace,
-- a non-numeric interval bound, a bound above the 1e15 limit, and a descending interval.
-- All four are decided while the table function arguments are parsed, before a connection
-- is attempted, so the test asserts only the error code and needs no MySQL server.

-- An opening brace that is never closed.
SELECT * FROM mysql('{abc:3306', 'db', 't', 'u', 'p'); -- { serverError BAD_ARGUMENTS }

-- The right bound of the interval is not a number.
SELECT * FROM mysql('{1..x}:3306', 'db', 't', 'u', 'p'); -- { serverError BAD_ARGUMENTS }

-- The right bound is numeric, but above the limit of 1e15.
SELECT * FROM mysql('{1..99999999999999999}:3306', 'db', 't', 'u', 'p'); -- { serverError BAD_ARGUMENTS }

-- The interval descends: the left bound is greater than the right one.
SELECT * FROM mysql('{5..1}:3306', 'db', 't', 'u', 'p'); -- { serverError BAD_ARGUMENTS }
