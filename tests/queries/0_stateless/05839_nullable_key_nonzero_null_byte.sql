-- Any nonzero byte of the null map means NULL, not only 1.
-- The data in the `Native` format has two rows with the null map bytes 0x02 for the column `a`,
-- so both rows are (NULL, 5), although the nested values under NULL are different (11 and 22).

SELECT a, b FROM format(Native, 'a Nullable(UInt8), b UInt8',
    unhex('020201610f4e756c6c61626c652855496e74382902020b1601620555496e74380505'))
ORDER BY ALL;

-- Several fixed-size keys are packed into a single key with a bitmap of NULLs.
SELECT a, b, count() FROM format(Native, 'a Nullable(UInt8), b UInt8',
    unhex('020201610f4e756c6c61626c652855496e74382902020b1601620555496e74380505'))
GROUP BY a, b
ORDER BY ALL;

SELECT DISTINCT a, b FROM format(Native, 'a Nullable(UInt8), b UInt8',
    unhex('020201610f4e756c6c61626c652855496e74382902020b1601620555496e74380505'))
ORDER BY ALL;

SELECT count() FROM format(Native, 'a Nullable(UInt8), b UInt8',
    unhex('020201610f4e756c6c61626c652855496e74382902020b1601620555496e74380505'))
WHERE (a, b) IN (SELECT a, b FROM format(Native, 'a Nullable(UInt8), b UInt8',
    unhex('020201610f4e756c6c61626c652855496e74382902020b1601620555496e74380505')))
SETTINGS transform_null_in = 1;
