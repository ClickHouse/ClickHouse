-- Obsolete access types are marked in `system.privileges` and `system.grants`,
-- and they remain members of their parent groups, so revoking a group removes them.

SELECT privilege, is_obsolete FROM system.privileges WHERE is_obsolete ORDER BY privilege;

DROP USER IF EXISTS u_05836_obsolete;
CREATE USER u_05836_obsolete;

GRANT SYSTEM RELOAD MODEL ON *.* TO u_05836_obsolete;
SELECT access_type, is_obsolete FROM system.grants WHERE user_name = 'u_05836_obsolete';
REVOKE SYSTEM ON *.* FROM u_05836_obsolete;
SELECT count() FROM system.grants WHERE user_name = 'u_05836_obsolete';

GRANT SYSTEM RELOAD MODEL ON *.* TO u_05836_obsolete;
REVOKE ALL ON *.* FROM u_05836_obsolete;
SELECT count() FROM system.grants WHERE user_name = 'u_05836_obsolete';

GRANT SELECT ON *.* TO u_05836_obsolete;
SELECT access_type, is_obsolete FROM system.grants WHERE user_name = 'u_05836_obsolete';

DROP USER u_05836_obsolete;
