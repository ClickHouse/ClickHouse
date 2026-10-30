-- A forwarding chain that leaves an `Overlay` database and comes back walks through a temporary: the
-- overlay hands out a fresh `StorageAlias` on every lookup and owns none of them. Resolving the blocker
-- owner must not confuse a freed temporary with a visited storage, or the stop lands on an object that
-- is about to be destroyed: the table that really owns the blocker reports nothing, and the stop can
-- never be released.

DROP DATABASE IF EXISTS stopped_actions_chain_ov SYNC;
DROP DATABASE IF EXISTS stopped_actions_chain_src SYNC;
CREATE DATABASE stopped_actions_chain_src;
CREATE DATABASE stopped_actions_chain_ov ENGINE = Overlay(stopped_actions_chain_src);

-- The aliases name their targets before the targets exist, so every hop is a lazy stand-in.
CREATE TABLE stopped_actions_chain_src.a ENGINE = Alias('stopped_actions_chain_ov', 'b');
CREATE TABLE stopped_actions_chain_src.b ENGINE = Alias('stopped_actions_chain_ov', 't');
CREATE TABLE stopped_actions_chain_src.t (x UInt64) ENGINE = MergeTree ORDER BY x;

-- Stopping through the outermost alias reaches `t` through `ov.b` and `ov.t`.
SYSTEM STOP MERGES stopped_actions_chain_src.a;
SELECT stopped_actions FROM system.tables WHERE database = 'stopped_actions_chain_src' AND name = 't';

-- So the same entry is released by naming `t` directly.
SYSTEM START MERGES stopped_actions_chain_src.t;
SELECT stopped_actions FROM system.tables WHERE database = 'stopped_actions_chain_src' AND name = 't';

-- Merges really run again: this throws `ABORTED` if the stop was left behind.
INSERT INTO stopped_actions_chain_src.t VALUES (1);
INSERT INTO stopped_actions_chain_src.t VALUES (2);
OPTIMIZE TABLE stopped_actions_chain_src.t FINAL;
SELECT count() FROM stopped_actions_chain_src.t;

DROP DATABASE stopped_actions_chain_ov SYNC;
DROP DATABASE stopped_actions_chain_src SYNC;
