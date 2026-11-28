-- A standalone `ALTER USER SELF AUTHENTICATION` is formatted without an object scope, after scoped entries.
-- Each statement is printed formatted, together with whether formatting is idempotent.

SELECT formatQuerySingleLine(q), formatQuerySingleLine(q) = formatQuerySingleLine(formatQuerySingleLine(q))
FROM
(
    SELECT arrayJoin([
        'GRANT ALTER USER SELF AUTHENTICATION TO u',
        'GRANT ALTER USER SELF AUTHENTICATION ON CLUSTER a_cluster TO u',
        'GRANT ALTER USER SELF AUTHENTICATION, ALTER USER ON *.* TO u WITH GRANT OPTION',
        'GRANT ALTER USER, ALTER USER SELF AUTHENTICATION ON *.* TO u WITH GRANT OPTION',
        'REVOKE GRANT OPTION FOR ALTER USER SELF AUTHENTICATION, ALTER USER ON *.* FROM u'
    ]) AS q
);
