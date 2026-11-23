-- `ADD ENUM VALUES` through an Alias numbers the elements written without a number after the target's largest value.

DROP TABLE IF EXISTS al_chain;
DROP TABLE IF EXISTS al;
DROP TABLE IF EXISTS tgt;

CREATE TABLE tgt (x UInt64, e Enum8('old' = 10), n Nullable(Enum16('old' = 1))) ENGINE = MergeTree ORDER BY x;
-- `al_chain` is created before `al`, because an Alias of an existing Alias is refused.
CREATE TABLE al_chain ENGINE = Alias('al');
CREATE TABLE al ENGINE = Alias('tgt');

ALTER TABLE al MODIFY COLUMN e ADD ENUM VALUES ('a');
ALTER TABLE al_chain MODIFY COLUMN e ADD ENUM VALUES ('b', 'c');
ALTER TABLE al MODIFY COLUMN n ADD ENUM VALUES ('a');
ALTER TABLE al_chain MODIFY COLUMN n ADD ENUM VALUES ('b', 'c');

SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'tgt' AND name IN ('e', 'n') ORDER BY name;

DROP TABLE al_chain;
DROP TABLE al;
DROP TABLE tgt;
