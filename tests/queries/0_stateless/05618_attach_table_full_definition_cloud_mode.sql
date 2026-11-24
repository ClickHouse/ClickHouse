-- in cloud_mode, ATTACH TABLE with a full definition must not create a new table

CREATE TABLE t_detached (x UInt32) ENGINE = Memory;
DETACH TABLE t_detached;

SET cloud_mode = 1;

ATTACH TABLE t_new (x UInt32) ENGINE = Log; -- { serverError SUPPORT_IS_DISABLED }
ATTACH TABLE t_new (x UInt32) ENGINE = Memory; -- { serverError SUPPORT_IS_DISABLED }
ATTACH TABLE t_new UUID '0d7a2f61-5d3b-4c55-9b8e-05618a77ac01' (x UInt32) ENGINE = Memory; -- { serverError SUPPORT_IS_DISABLED }
ATTACH TABLE t_new FROM '/nonexistent' (x UInt32) ENGINE = Log; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_new';

-- the short syntax still attaches a detached table
ATTACH TABLE t_detached;
SELECT count() FROM t_detached;

SET cloud_mode = 0;
DROP TABLE t_detached;
