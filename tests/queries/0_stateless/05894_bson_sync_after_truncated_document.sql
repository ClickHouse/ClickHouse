-- A truncated document makes the reader fail before the size of the document is read. `syncAfterError` then has nothing
-- of the document to skip, and must not compute a negative remainder (UBSan: unsigned integer overflow).

SET input_format_parallel_parsing = 0;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('01')) SETTINGS input_format_allow_errors_num = 1;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0100')) SETTINGS input_format_allow_errors_num = 1;
SELECT 'ok';
