-- A column added under the name of a subcolumn and renamed in the same `ALTER` is still read under its old name from
-- the parts that miss it, so a sorting key over the new name would read the subcolumn there.

DROP TABLE IF EXISTS t_modify_order_by_rename;
CREATE TABLE t_modify_order_by_rename (id UInt8, a Array(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_modify_order_by_rename VALUES (1, [1, 2, 3]), (1, [1]);

ALTER TABLE t_modify_order_by_rename
    ADD COLUMN `a.size0` UInt64,
    RENAME COLUMN `a.size0` TO `a.sz`,
    MODIFY ORDER BY (id, `a.sz`); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- The same without the shadowing name is allowed.
ALTER TABLE t_modify_order_by_rename
    ADD COLUMN b UInt64,
    RENAME COLUMN b TO c,
    MODIFY ORDER BY (id, c);

SELECT id, a, c FROM t_modify_order_by_rename ORDER BY id, c, a SETTINGS optimize_read_in_order = 1;
SELECT id, a, c FROM t_modify_order_by_rename ORDER BY id, c, a SETTINGS optimize_read_in_order = 0;

DROP TABLE t_modify_order_by_rename;
