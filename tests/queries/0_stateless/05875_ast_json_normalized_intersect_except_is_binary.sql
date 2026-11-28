-- A normalized `SelectWithUnionQuery` is skipped by the visitors that lower `INTERSECT` / `EXCEPT`
-- to binary nodes, so its `union_mode` reaches the planner as is, and the step for `INTERSECT` /
-- `EXCEPT` takes exactly two inputs. A normalized node with such a mode and more than two selects
-- used to be accepted by the AST JSON reader and failed with a logical error during execution.

-- Three operands: rejected for every explicit `INTERSECT` / `EXCEPT` mode.
SELECT formatQueryFromJSON(replace(replace(
    parseQueryToJSON($$SELECT 1 EXCEPT ALL SELECT 2 EXCEPT ALL SELECT 3$$),
    '"union_mode":"UNION_DEFAULT","is_normalized":false,"list_of_modes":["EXCEPT_ALL","EXCEPT_ALL"],',
    '"union_mode":"{mode}","is_normalized":true,'), '{mode}', 'EXCEPT_ALL')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(replace(
    parseQueryToJSON($$SELECT 1 EXCEPT ALL SELECT 2 EXCEPT ALL SELECT 3$$),
    '"union_mode":"UNION_DEFAULT","is_normalized":false,"list_of_modes":["EXCEPT_ALL","EXCEPT_ALL"],',
    '"union_mode":"{mode}","is_normalized":true,'), '{mode}', 'EXCEPT_DISTINCT')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(replace(
    parseQueryToJSON($$SELECT 1 EXCEPT ALL SELECT 2 EXCEPT ALL SELECT 3$$),
    '"union_mode":"UNION_DEFAULT","is_normalized":false,"list_of_modes":["EXCEPT_ALL","EXCEPT_ALL"],',
    '"union_mode":"{mode}","is_normalized":true,'), '{mode}', 'INTERSECT_ALL')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(replace(
    parseQueryToJSON($$SELECT 1 EXCEPT ALL SELECT 2 EXCEPT ALL SELECT 3$$),
    '"union_mode":"UNION_DEFAULT","is_normalized":false,"list_of_modes":["EXCEPT_ALL","EXCEPT_ALL"],',
    '"union_mode":"{mode}","is_normalized":true,'), '{mode}', 'INTERSECT_DISTINCT')); -- { serverError BAD_ARGUMENTS }

-- The matching `list_of_modes` does not make it valid: a normalized node does not read it.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON($$SELECT 1 EXCEPT ALL SELECT 2 EXCEPT ALL SELECT 3$$),
    '"union_mode":"UNION_DEFAULT","is_normalized":false,',
    '"union_mode":"EXCEPT_ALL","is_normalized":true,')); -- { serverError BAD_ARGUMENTS }

-- Two operands are the form the query tree writes for a binary set operation: still accepted.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON($$SELECT 1 EXCEPT ALL SELECT 2$$),
    '"union_mode":"UNION_DEFAULT","is_normalized":false,"list_of_modes":["EXCEPT_ALL"],',
    '"union_mode":"EXCEPT_ALL","is_normalized":true,'));

-- A normalized `UNION` keeps any number of selects.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON($$SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3$$),
    '"union_mode":"UNION_DEFAULT","is_normalized":false,"list_of_modes":["UNION_ALL","UNION_ALL"],',
    '"union_mode":"UNION_ALL","is_normalized":true,'));

-- The non-normalized chain is lowered to binary nodes by the visitors, as before.
SELECT formatQueryFromJSON(parseQueryToJSON($$SELECT 1 EXCEPT ALL SELECT 2 EXCEPT ALL SELECT 3$$));
