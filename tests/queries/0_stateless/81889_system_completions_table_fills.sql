CREATE DATABASE IF NOT EXISTS `aaadatabase`;
USE `aaadatabase`;

CREATE TABLE IF NOT EXISTS `aaatable` (
    `aaafoo`    String,
    `aaabar`    UInt16,
    `aaabaz`    UInt128
) ENGINE = Memory;

SELECT DISTINCT lower(word) AS term
FROM system.completions
ORDER BY term
LIMIT 20
FORMAT PrettyCompact;

