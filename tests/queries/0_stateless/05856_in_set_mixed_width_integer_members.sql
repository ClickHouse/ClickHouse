-- Each fixture holds the values a wrapping or rounding conversion of the mixed-width `IN` literals would produce.

SELECT arraySort(groupArray(n)) FROM values('n UInt8', 1, 44, 255) WHERE n IN (1, 300, -1);
SELECT arraySort(groupArray(n)) FROM values('n UInt8', 1, 44, 255) WHERE n NOT IN (1, 300, -1);
SELECT arraySort(groupArray(n)) FROM values('n Int8', 5, -128, 127) WHERE n IN (5, 128, -129);
SELECT arraySort(groupArray(n)) FROM values('n UInt32', 0, 1, 256, 4294967295) WHERE n IN (4294967295, 4294967296, 256, 1);
SELECT arraySort(groupArray(n)) FROM values('n UInt64', 0, 1, 255, 65536, 18446744073709551615) WHERE n IN (0, 255, 65536, 18446744073709551615);
SELECT arraySort(groupArray(n)) FROM values('n UInt64', 1, 18446744073709551615) WHERE n IN (-1, 18446744073709551615, 1);
SELECT arraySort(groupArray(n)) FROM values('n Int64', -9223372036854775808, 0, 1, 4294967295) WHERE n IN (-9223372036854775808, 4294967295, 1);
SELECT arraySort(groupArray(n)) FROM values('n Float32', -1, 300, 16777216) WHERE n IN (-1, 16777217, 300);
SELECT arraySort(groupArray(n)) FROM values('n Float64', 1, 300, 9007199254740992) WHERE n IN (1, 9007199254740993, 300);
SELECT arraySort(groupArray(n)) FROM values('n UInt16', 1, 2, 3, 300) WHERE n IN (1, 2.5, 300);
SELECT n FROM values('n Nullable(UInt16)', 1, NULL, 44, 300) WHERE n IN (1, NULL, 300) ORDER BY n SETTINGS transform_null_in = 0;
SELECT n FROM values('n Nullable(UInt16)', 1, NULL, 44, 300) WHERE n IN (1, NULL, 300) ORDER BY n SETTINGS transform_null_in = 1;
