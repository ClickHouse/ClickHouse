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
-- The semi and anti kinds return only the columns of the side they keep.

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
| sort by LK asc;

print '-- kind=rightsemi keeps the right rows that match, right columns only --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=rightsemi (datatable (RK:long, R:string) [2, 'p', 3, 'q']) on $left.LK == $right.RK
| sort by RK asc;

print '-- kind=leftanti keeps the left rows that do not match, left columns only --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=leftanti (datatable (RK:long, R:string) [2, 'p', 3, 'q']) on $left.LK == $right.RK
| sort by LK asc;

print '-- kind=rightanti keeps the right rows that do not match, right columns only --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=rightanti (datatable (RK:long, R:string) [2, 'p', 3, 'q']) on $left.LK == $right.RK
| sort by RK asc;

print '-- leftantisemi is the same kind under another name --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=leftantisemi (datatable (RK:long, R:string) [2, 'p', 3, 'q']) on $left.LK == $right.RK
| sort by LK asc;

-- One line: a KQL parse error is reported by the client, which looks for the
-- hint on the first line of the statement it could not parse.
print '-- the set of kind names is closed --';
datatable (K:long) [1] | join kind=sideways (datatable (K:long) [1]) on K; -- { clientError SYNTAX_ERROR }

-- The remaining blocks are the cases where the kept side is easy to get wrong: a
-- join inside the right input, which must not change the aliases of the outer join,
-- and inputs that share column names, where the result must keep the plain names of
-- the kept side so that a later `project`, `where` or `sort` can still refer to them.

print '-- a join inside the right input keeps the aliases of the outer leftsemi join --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=leftsemi (datatable (RK:long, R:string) [2, 'p', 3, 'q'] | join kind=inner (datatable (SK:long, S:string) [2, 'x', 3, 'y']) on $left.RK == $right.SK) on $left.LK == $right.RK
| sort by LK asc;

print '-- a join inside the right input keeps the aliases of the outer rightanti join --';
datatable (LK:long, L:string) [1, 'a', 2, 'b']
| join kind=rightanti (datatable (RK:long, R:string) [2, 'p', 3, 'q'] | join kind=inner (datatable (SK:long, S:string) [2, 'x', 3, 'y']) on $left.RK == $right.SK) on $left.LK == $right.RK
| sort by RK asc;

print '-- leftsemi with the same column names on both sides, then project --';
datatable (K:long, V:string) [1, 'a', 2, 'b']
| join kind=leftsemi (datatable (K:long, V:string) [2, 'p', 3, 'q']) on K
| project K, V
| sort by K asc;

print '-- rightsemi with the same column names on both sides, then project --';
datatable (K:long, V:string) [1, 'a', 2, 'b']
| join kind=rightsemi (datatable (K:long, V:string) [2, 'p', 3, 'q']) on K
| project K, V
| sort by K asc;

print '-- leftanti with the same column names on both sides, then project --';
datatable (K:long, V:string) [1, 'a', 2, 'b']
| join kind=leftanti (datatable (K:long, V:string) [2, 'p', 3, 'q']) on K
| project K, V
| sort by K asc;

print '-- rightanti with the same column names on both sides, then project --';
datatable (K:long, V:string) [1, 'a', 2, 'b']
| join kind=rightanti (datatable (K:long, V:string) [2, 'p', 3, 'q']) on K
| project K, V
| sort by K asc;

print '-- rightsemi with the same column names on both sides, then where --';
datatable (K:long, V:string) [1, 'a', 2, 'b', 3, 'c']
| join kind=rightsemi (datatable (K:long, V:string) [2, 'p', 3, 'q', 4, 'r']) on K
| where V != 'p'
| sort by K asc;

print '-- rightanti with the same column names on both sides sorts by the right key --';
datatable (K:long, V:string) [1, 'a', 2, 'b']
| join kind=rightanti (datatable (K:long, V:string) [4, 'r', 2, 'p', 3, 'q']) on K
| sort by K asc;
