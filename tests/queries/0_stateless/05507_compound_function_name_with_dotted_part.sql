-- A part of a compound function name can contain dots; the formatted query must parse back.
SELECT formatQuerySingleLine(q) AS f, formatQuerySingleLine(f) = f
FROM (SELECT arrayJoin([
    'SELECT `a..b`.f(1)',
    'SELECT * FROM `.db`.pv(p = 1)',
    'SELECT * FROM `db.`.pv(p = 1)',
    'SELECT * FROM db.`pv.`(p = 1)',
    'SELECT * FROM `127.0.0.{4..7}`.pv(p = 1)']) AS q);

-- A debug build checks the same round trip before running a query.
SELECT `a..b`.f(1); -- { serverError UNKNOWN_FUNCTION }
