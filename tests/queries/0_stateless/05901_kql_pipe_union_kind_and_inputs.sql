-- The pipe form `<source> | union ...` is the only path into `KQLParser::parseUnion`:
-- a query that *starts* with `union` is handled by a separate branch of `parseSource`.
-- Both input forms of the operator are covered here, together with the `kind=` option.
-- `union kind=` is listed among the constructs this dialect deliberately does not support
-- (docs/guides/clickhouse/kusto-query-language.mdx, "Operator options"), so rejecting it
-- is the documented behaviour. Only the error code name is asserted, not the message.

SET enable_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- parenthesized tabular expressions as union inputs --';
datatable (n:long) [11] | union (datatable (n:long) [22]), (datatable (n:long) [33]) | sort by n asc;

print '-- a bare tabular source as a union input --';
datatable (n:long) [44] | union datatable (n:long) [55] | sort by n asc;

print '-- both input forms in one union --';
datatable (n:long) [66] | union (datatable (n:long) [77]), datatable (n:long) [88] | sort by n asc;

print '-- `kind=outer` and `kind=inner` are recognised, but not implemented --';
datatable (n:long) [1] | union kind=outer (datatable (n:long) [2]); -- { clientError SYNTAX_ERROR }
datatable (n:long) [1] | union kind=inner (datatable (n:long) [2]); -- { clientError SYNTAX_ERROR }

print '-- any other word is not a union kind at all --';
datatable (n:long) [1] | union kind=leftouter (datatable (n:long) [2]); -- { clientError SYNTAX_ERROR }

SET dialect = 'clickhouse';
