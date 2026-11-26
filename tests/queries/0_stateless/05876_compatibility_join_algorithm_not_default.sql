-- `compatibility` with a version before 24.12 keeps `join_algorithm` at 'direct,parallel_hash,hash' (read back as 'direct,hash', since `parallel_hash` is an alias of `hash`) instead of restoring the deprecated 'default'.

SET compatibility = '23.8';
SELECT value FROM system.settings WHERE name = 'join_algorithm';

SET compatibility = '24.11';
SELECT value FROM system.settings WHERE name = 'join_algorithm';

SET compatibility = '24.12';
SELECT value FROM system.settings WHERE name = 'join_algorithm';

SET compatibility = '26.7';
SELECT value FROM system.settings WHERE name = 'join_algorithm';

-- An explicit 'default' is still accepted and kept.
SET join_algorithm = 'default';
SET compatibility = '24.11';
SELECT value FROM system.settings WHERE name = 'join_algorithm';
