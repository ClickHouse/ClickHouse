-- A table with the old engine syntax does not read index declarations, so a lookup index must be rejected
-- instead of being silently ignored.

SET allow_experimental_lookup_index = 1;
SET allow_deprecated_syntax_for_merge_tree = 1;

DROP TABLE IF EXISTS lookup_old_syntax;

CREATE TABLE lookup_old_syntax (d Date, id UInt64, LOOKUP INDEX s (id) TYPE table_set) ENGINE = MergeTree(d, id, 8192); -- { serverError BAD_ARGUMENTS }

CREATE TABLE lookup_old_syntax (d Date, id UInt64) ENGINE = MergeTree(d, id, 8192);
ALTER TABLE lookup_old_syntax ADD LOOKUP INDEX s (id) TYPE table_set; -- { serverError BAD_ARGUMENTS }

DETACH TABLE lookup_old_syntax;
ATTACH TABLE lookup_old_syntax;
SHOW CREATE TABLE lookup_old_syntax FORMAT TSVRaw;

DROP TABLE lookup_old_syntax;
