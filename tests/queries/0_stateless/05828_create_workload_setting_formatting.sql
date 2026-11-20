-- The parser reads the name of a workload setting as one identifier.
-- The formatted query must keep the quotes of a name that is not a plain identifier, otherwise it is not parseable.
-- The second column shows that the formatted query is parseable and that the format is stable.

SELECT formatQuerySingleLine('CREATE WORKLOAD w SETTINGS `e f` = 1') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('CREATE WORKLOAD w SETTINGS `e.f` = 1') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('CREATE WORKLOAD w IN `p q` SETTINGS `e.f` = 1 FOR `r s`, weight = 2 FOR `a.b`, max_io_requests = 3') AS f, formatQuerySingleLine(f) = f;

-- The format of a plain name does not change.
SELECT formatQuerySingleLine('CREATE WORKLOAD w IN p SETTINGS weight = 2, priority = 1 FOR r') AS f, formatQuerySingleLine(f) = f;
