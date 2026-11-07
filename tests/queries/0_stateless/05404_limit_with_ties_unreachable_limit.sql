-- `offset + limit` does not fit in `UInt64`: the limit is never reached, so `WITH TIES` must not evaluate the sum.
SELECT number FROM numbers(3) ORDER BY number LIMIT 1, 18446744073709551615 WITH TIES;
SELECT number FROM numbers(10) ORDER BY number LIMIT 5, 18446744073709551615 WITH TIES SETTINGS max_block_size = 3;
