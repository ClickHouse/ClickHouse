-- A dependency that is not in the backup is dropped from the restore order. Checking whether it exists must not
-- resolve its name in a database that reads the data on access.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Filesystem;
USE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.d_outside (id UInt64, value String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE '../outside.csv')) LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.d_absent (id UInt64, value String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'absent.csv')) LAYOUT(FLAT()) LIFETIME(0);
USE {CLICKHOUSE_DATABASE:Identifier};

BACKUP DICTIONARY d_outside, DICTIONARY d_absent TO Memory('b_05835') FORMAT Null;
DROP DICTIONARY d_outside SYNC;
DROP DICTIONARY d_absent SYNC;
RESTORE DICTIONARY d_outside, DICTIONARY d_absent FROM Memory('b_05835') FORMAT Null;
SELECT name FROM system.dictionaries WHERE database = currentDatabase() ORDER BY name;

-- The restored dictionary still cannot read a file outside `user_files`.
SELECT dictGet('d_outside', 'value', toUInt64(1)); -- { serverError PATH_ACCESS_DENIED }

DROP DICTIONARY d_outside;
DROP DICTIONARY d_absent;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
