-- The estimates of the numbers of distinct values from metadata are disabled by `compatibility` with the versions
-- that did not have them.

SELECT value FROM system.settings WHERE name = 'estimate_distinct_values_from_metadata';
SET compatibility = '26.11';
SELECT value FROM system.settings WHERE name = 'estimate_distinct_values_from_metadata';
