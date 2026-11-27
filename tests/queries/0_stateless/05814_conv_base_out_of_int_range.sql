SELECT conv('ff', 4294967312, 10); -- { serverError BAD_ARGUMENTS }
SELECT conv('ff', -4294967280, 10); -- { serverError BAD_ARGUMENTS }
SELECT conv('ff', 16, 4294967306); -- { serverError BAD_ARGUMENTS }
SELECT conv('11', 4294967298, 10); -- { serverError BAD_ARGUMENTS }
SELECT conv('ff', toUInt64(4294967312), 10); -- { serverError BAD_ARGUMENTS }
SELECT conv('ff', materialize(toUInt64(4294967312)), 10); -- { serverError BAD_ARGUMENTS }
SELECT conv('ff', 16, 10), conv('ff', toInt64(16), toInt64(10)), conv('11', 2, 36);
