-- The `Lance` parser cannot represent `-9223372036854775808` as an `Int64` literal, so such a value
-- must not be pushed down. The results must not depend on `lance_enable_predicate_pushdown`.
DROP TABLE IF EXISTS lance_local_integer_literal_bounds;

CREATE TABLE lance_local_integer_literal_bounds
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/multi_frag.lance');

SELECT 'pushdown enabled';
SET lance_enable_predicate_pushdown = 1;
SELECT count() FROM lance_local_integer_literal_bounds WHERE id > -9223372036854775808;
SELECT count() FROM lance_local_integer_literal_bounds WHERE id != -9223372036854775808;
SELECT count() FROM lance_local_integer_literal_bounds WHERE id = -9223372036854775808;
SELECT count() FROM lance_local_integer_literal_bounds WHERE id IN (-9223372036854775808, 1, 2);
SELECT count() FROM lance_local_integer_literal_bounds WHERE id > -9223372036854775808 AND id < 5;
SELECT count() FROM lance_local_integer_literal_bounds WHERE id < 9223372036854775807;
SELECT count() FROM lance_local_integer_literal_bounds WHERE id > -9223372036854775807;

SELECT 'pushdown disabled';
SET lance_enable_predicate_pushdown = 0;
SELECT count() FROM lance_local_integer_literal_bounds WHERE id > -9223372036854775808;
SELECT count() FROM lance_local_integer_literal_bounds WHERE id != -9223372036854775808;
SELECT count() FROM lance_local_integer_literal_bounds WHERE id = -9223372036854775808;
SELECT count() FROM lance_local_integer_literal_bounds WHERE id IN (-9223372036854775808, 1, 2);
SELECT count() FROM lance_local_integer_literal_bounds WHERE id > -9223372036854775808 AND id < 5;
SELECT count() FROM lance_local_integer_literal_bounds WHERE id < 9223372036854775807;
SELECT count() FROM lance_local_integer_literal_bounds WHERE id > -9223372036854775807;

DROP TABLE lance_local_integer_literal_bounds;
