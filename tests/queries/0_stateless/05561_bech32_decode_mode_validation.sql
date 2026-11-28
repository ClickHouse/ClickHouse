-- Tags: no-fasttest
-- Tag no-fasttest: `bech32Decode` is gated behind `USE_BECH32`, which is not enabled in the Fast test build.

-- The optional second argument of `bech32Decode` selects the decode mode. Per the function's own
-- documentation (the `FunctionDocumentation` block in src/Functions/bech32.cpp), the argument is
-- "Optional. Pass `'raw'` to decode without stripping the first byte as a witness version. Use this
-- for non-SegWit addresses (e.g. Cosmos SDK)."
-- It is a mode selector, not per-row data, so it has to be one constant `'raw'` for the whole block.
-- 04035_bech32_bip173_raw_encoding covers the single-row constant form only; the two validation
-- branches below - an empty block, and a mode that is not the same in every row - are what happens
-- when the mode arrives as a column instead.

SELECT 'raw mode decodes a non-SegWit address';
-- A Cosmos-style bech32 string whose 32 data characters carry exactly 160 bits. Raw mode keeps all
-- of them and returns 20 whole bytes, while the default SegWit mode spends the first 5-bit group on
-- the witness version and is left with 155 bits, i.e. no whole payload at all.
SELECT r.1, hex(r.2) FROM (SELECT bech32Decode('cosmos1w508d6qejxtdg4y5r3zarvary0c5xw7k6ah60c', 'raw') AS r);
SELECT bech32Decode('cosmos1w508d6qejxtdg4y5r3zarvary0c5xw7k6ah60c');

SELECT 'an empty block still validates a constant mode';
-- An empty block has no row to read the mode from, so the check falls back to the constant behind
-- the column: `'raw'` passes and decodes nothing, any other value is rejected even though not a
-- single address is decoded. `materialize` on the address keeps the call out of the
-- all-arguments-constant fast path, so the mode really does reach the function as a column.
SELECT count() FROM (SELECT bech32Decode(materialize('cosmos1w508d6qejxtdg4y5r3zarvary0c5xw7k6ah60c'), 'raw') FROM numbers(0));
SELECT bech32Decode(materialize('cosmos1w508d6qejxtdg4y5r3zarvary0c5xw7k6ah60c'), 'RAW') FROM numbers(0); -- { serverError BAD_ARGUMENTS }

SELECT 'a mode that differs between rows is rejected';
-- `arrayJoin` expands one input row into both output rows of a single block, so the function sees
-- two different modes in one call and cannot pick either of them.
SELECT bech32Decode(materialize('cosmos1w508d6qejxtdg4y5r3zarvary0c5xw7k6ah60c'), if(x = 0, 'raw', 'x')) FROM (SELECT arrayJoin([0, 1]) AS x); -- { serverError BAD_ARGUMENTS }
