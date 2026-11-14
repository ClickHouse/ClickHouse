-- `COMMENT COLUMN` on a parameterized view with a declared schema must be persisted
-- in the stored definition and survive `DETACH`/`ATTACH`.

SET use_declared_schema_for_parameterized_views = 1;

DROP VIEW IF EXISTS pv_alter_comment;
CREATE VIEW pv_alter_comment (n UInt64 COMMENT 'before') AS SELECT number AS n FROM numbers({p:UInt64});

ALTER TABLE pv_alter_comment COMMENT COLUMN n 'after';
SELECT name, type, comment FROM system.columns WHERE database = currentDatabase() AND table = 'pv_alter_comment';
SELECT position(create_table_query, 'after') > 0, position(create_table_query, 'before') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'pv_alter_comment';

DETACH TABLE pv_alter_comment;
ATTACH TABLE pv_alter_comment;
SELECT name, type, comment FROM system.columns WHERE database = currentDatabase() AND table = 'pv_alter_comment';
SELECT * FROM pv_alter_comment(p = 3);

DROP VIEW pv_alter_comment;
