--  max_memory_usage = 10000000000 (10 GB default)
--  Intel® Xeon® E5-1650 v3 Hexadcore 128 GB DDR4 ECC
--  Estimated time: ~ 250 seconds
--  Read rows:      ~ 272 000 000
SELECT
  key,
  uniqState(uuid_1) uuid_1_st,
  uniqState(uuid_2) uuid_2_st,
  uniqState(uuid_3) uuid_3_st
FROM (
  SELECT
    rand64() value,
    toString(value) value_str,
    UUIDNumToString(toFixedString(substring(value_str, 1, 16), 16)) uuid_1, -- Any UUID
    UUIDNumToString(toFixedString(substring(value_str, 2, 16), 16)) uuid_2, -- More memory
    UUIDNumToString(toFixedString(substring(value_str, 3, 16), 16)) uuid_3, -- And more memory
    modulo(value, 5000000) key -- Cardinality in my case
  FROM numbers(550000000)
)
GROUP BY
  key
LIMIT 100;
