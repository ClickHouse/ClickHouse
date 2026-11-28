-- The six `join kind=` values that no other test uses, i.e. the remaining arms
-- of the `switch (op.join_kind)` in `applyJoin`, src/Parsers/Kusto/KQLTranslator.cpp.
-- `innerunique`, `inner` and `leftouter` are already covered by
-- 04700_kql_join_let_bin_regressions and 04670_kql_pipeline. Expected behaviour:
-- the Kusto `join` operator documentation - each kind keeps a different side of
-- the rows that have no match.
--
-- Every block joins the same two literals: key 2 matches, key 1 is left-only and
-- key 3 is right-only, so each kind has a different answer. The two keys are
-- named differently so both stay addressable: this dialect never emits `USING`,
-- so a kind that returns both sides keeps both key columns - four columns a row.
--
-- Known translation defect: in Kusto the semi and anti kinds return only the
-- retained side's columns, but `applyJoin` only sets the join kind and strictness
-- and leaves the generated `SELECT *` over both inputs, so those kinds currently
-- also return the other side filled with defaults. Every semi and anti block
-- below therefore projects the retained side explicitly, so the reference asserts
-- the Kusto-correct columns only and keeps passing once the missing projection is
-- added to the translator.

-- Rows without a match must show the type default, not NULL, for the reference to
-- stay readable. That is the default; pin it anyway, and pin it before the
-- dialect switch because `SET` is not KQL.
SET join_use_nulls = 0;
SET enable_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- kind=rightouter keeps the right row without a match --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=rightouter (datatable (RK:long, R:string) [2, 'p', 3, 'q']) on $left.LK == $right.RK
| sort by RK asc;

print '-- kind=fullouter keeps the unmatched rows of both sides --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=fullouter (datatable (RK:long, R:string) [2, 'p', 3, 'q']) on $left.LK == $right.RK
| sort by LK asc;

print '-- kind=leftsemi keeps the left rows that match, left columns only --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=leftsemi (datatable (RK:long, R:string) [2, 'p', 3, 'q']) on $left.LK == $right.RK
| project LK, L
| sort by LK asc;

print '-- kind=rightsemi keeps the right rows that match, right columns only --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=rightsemi (datatable (RK:long, R:string) [2, 'p', 3, 'q']) on $left.LK == $right.RK
| project RK, R
| sort by RK asc;

print '-- kind=leftanti keeps the left rows that do not match, left columns only --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=leftanti (datatable (RK:long, R:string) [2, 'p', 3, 'q']) on $left.LK == $right.RK
| project LK, L
| sort by LK asc;

print '-- kind=rightanti keeps the right rows that do not match, right columns only --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=rightanti (datatable (RK:long, R:string) [2, 'p', 3, 'q']) on $left.LK == $right.RK
| project RK, R
| sort by RK asc;

print '-- leftantisemi is the same kind under another name --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=leftantisemi (datatable (RK:long, R:string) [2, 'p', 3, 'q']) on $left.LK == $right.RK
| project LK, L
| sort by LK asc;

-- One line: a KQL parse error is reported by the client, which looks for the
-- hint on the first line of the statement it could not parse.
print '-- the set of kind names is closed --';
datatable (K:long) [1] | join kind=sideways (datatable (K:long) [1]) on K; -- { clientError SYNTAX_ERROR }
