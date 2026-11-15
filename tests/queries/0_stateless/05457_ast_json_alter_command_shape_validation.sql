-- Coverage for the per-command shape validation in `ASTAlterCommand::readJSON`
-- (src/Parsers/ASTAlterQuery.cpp). `formatQueryFromJSON` (documented in the FunctionDocumentation
-- block of src/Functions/formatQueryFromJSON.cpp and in
-- docs/ru/reference/functions/regular-functions/other-functions.mdx#formatQueryFromJSON) rebuilds an
-- AST from JSON, so every ALTER command form the parser produces must round-trip unchanged, while a
-- shape the parser can never produce must fail closed with BAD_ARGUMENTS instead of reaching a null
-- member in `formatImpl` or an unchecked downcast during execution.

-- ---------------------------------------------------------------------------
-- Parser-produced command forms accepted by the `require(...)` arms of the per-type switch.
-- `MODIFY QUERY` restores a screened `ASTSelectWithUnionQuery` child, and `UNLOCK SNAPSHOT ... FROM`
-- / `EXECUTE ...` restore their argument as a raw child (those slots have no single parser-owned
-- node type). All of them must round-trip:
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MATERIALIZE COLUMN c'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY COMMENT \'c1\''));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER DATABASE db MODIFY COMMENT \'c2\''));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP INDEX i'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MATERIALIZE INDEX i'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP CONSTRAINT k'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP PROJECTION p'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MATERIALIZE PROJECTION p'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY QUERY SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY REFRESH EVERY 1 HOUR'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY SQL SECURITY DEFINER'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t UNLOCK SNAPSHOT \'s1\' FROM disk_backup(\'b1\')'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t EXECUTE cmd(1, 2)'));

-- ---------------------------------------------------------------------------
-- `ALTER UPDATE`: `MutationCommand::parse` downcasts every `update_assignments` child with
-- `->as<ASTAssignment>()`, so a list child that is not an assignment is rejected at the boundary.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t UPDATE a = 7 WHERE 1'), '"type":"Assignment","column_name":"a"', '"type":"Literal","value":{"field_type":"UInt64","value":7}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `MODIFY TTL` carries the list-of-`ASTTTLElement` contract of `ASTStorage`'s `ttl_table`:
-- `TTLDescription::getTTLFromAST` reads a child that is not an `ASTTTLElement` as a column TTL, so a
-- `ttl` slot that is not an expression list cannot be restored.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY TTL d'), '"ttl":{"type":"ExpressionList","children"', '"ttl":{"type":"Identifier","name":"d","unused_children"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `RESET SETTING` holds an expression list of setting-name identifiers; a literal there would format
-- as `RESET SETTING 1`, which `ParserAlterCommand` never produces.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t RESET SETTING s'), '{"type":"Identifier","name":"s"}', '{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `MODIFY COLUMN` has exactly one parser sub-form (`REMOVE` / `MODIFY SETTING` / `RESET SETTING` /
-- `ADD ENUM VALUES` / the plain modify), and `FIRST`/`AFTER` are mutually exclusive. `formatImpl`
-- prints only the first matching sub-form while `AlterCommand::parse` still copies the hidden
-- fields, so the formatted SQL would hide a reorder or a setting reset that still executes.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY COLUMN x MODIFY SETTING s = 1'), '"settings_changes":', '"remove_property":"TTL","settings_changes":')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY COLUMN x UInt16 AFTER y'), '"first":false', '"first":true')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `MATERIALIZE STATISTICS` is parsed with `ParserStatisticsDeclarationWithoutTypes`, so a `TYPE`
-- list on its declaration is parser-impossible and `formatImpl` has nowhere to print it.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MATERIALIZE STATISTICS a'), '"statistics_decl":{"type":"StatisticsDeclaration","columns"', '"statistics_decl":{"type":"StatisticsDeclaration","types":{"type":"ExpressionList","children":[{"type":"Function","name":"tdigest","no_empty_args":true}]},"columns"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `UNFREEZE WITH NAME '<name>'`: unlike `FREEZE`, the parser requires the backup name for the
-- all-partitions form too, and `formatImpl` emits `WITH NAME` only for a non-empty one.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t UNFREEZE WITH NAME \'b1\''), '"with_name":"b1"', '"with_name":""')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `MOVE PARTITION`: `writeJSON` emits `move_destination_type` only for this command, so it must be
-- present and must name a `DataDestinationType`; only DISK/VOLUME/SHARD/TABLE have a `MOVE` grammar
-- branch; and the destination that branch prints (`move_destination_name`, or `to_table` for
-- `TO TABLE`) must be non-empty, otherwise the formatted SQL cannot be parsed back.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MOVE PARTITION 1 TO DISK \'d1\''), '"move_destination_type":"DISK"', '"move_destination_type":"TAPE"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MOVE PARTITION 1 TO DISK \'d1\''), '"move_destination_type":"DISK",', '')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MOVE PARTITION 1 TO DISK \'d1\''), '"move_destination_type":"DISK"', '"move_destination_type":"DELETE"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MOVE PARTITION 1 TO DISK \'d1\''), '"move_destination_name":"d1"', '"move_destination_name":""')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MOVE PARTITION 1 TO TABLE dst'), '"to_table":"dst"', '"to_table":""')); -- { serverError BAD_ARGUMENTS }
