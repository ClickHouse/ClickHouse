-- Tags: no-parallel
-- Columns cannot be granted on a wildcard: this is reported as `INVALID_GRANT`, not as a syntax error.

DROP USER IF EXISTS user_05513;
CREATE USER user_05513;

GRANT SELECT(a) ON db_05513.* TO user_05513; -- { clientError INVALID_GRANT }
GRANT SELECT(a, b, c) ON *.* TO user_05513; -- { clientError INVALID_GRANT }
GRANT SELECT(a) ON db_05513.t* TO user_05513; -- { clientError INVALID_GRANT }
GRANT SELECT(a) ON * TO user_05513; -- { clientError INVALID_GRANT }
CHECK GRANT SELECT(a) ON db_05513.*; -- { clientError INVALID_GRANT }
SELECT formatQuery('GRANT SELECT(a) ON db_05513.* TO user_05513'); -- { serverError INVALID_GRANT }

-- Columns on a table are fine.
GRANT SELECT(a) ON db_05513.t TO user_05513;
SHOW GRANTS FOR user_05513;
REVOKE SELECT(a) ON db_05513.t FROM user_05513;
SELECT '---';

-- Revoking columns on a wildcard is rejected too and must not revoke the whole privilege.
GRANT SELECT ON db_05513.* TO user_05513;
REVOKE SELECT(a) ON db_05513.* FROM user_05513; -- { clientError INVALID_GRANT }
REVOKE SELECT(a) ON db_05513.t* FROM user_05513; -- { clientError INVALID_GRANT }
SHOW GRANTS FOR user_05513;

DROP USER user_05513;
