-- A block without rows gets the structure of the header like any other block: the columns it lacks are added,
-- the others are put in the order of the header, and a block with only unknown columns does not end the data.
-- Each payload is such a block followed by a block of one row.

SELECT * FROM format(Native, 'x UInt32, y UInt32 DEFAULT x + 1, z String DEFAULT \'z\'', x'010001780655496e743332010101780655496e74333207000000');
SELECT * FROM format(Native, 'x UInt32, y String', x'0200017906537472696e6701780655496e7433320201017906537472696e67016101780655496e74333207000000');
SELECT * FROM format(Native, 'x UInt32', x'010001750655496e743332010101780655496e74333207000000');

SELECT * FROM format(NativeWithDefaults, 'x UInt32, y UInt32 DEFAULT x + 1, z String DEFAULT \'z\'', x'010001780655496e74333200010101780655496e7433320007000000');
SELECT * FROM format(NativeWithDefaults, 'x UInt32, y String', x'0200017906537472696e670001780655496e743332000201017906537472696e6700016101780655496e7433320007000000');
SELECT * FROM format(NativeWithDefaults, 'x UInt32', x'010001750655496e74333200010101780655496e7433320007000000');
