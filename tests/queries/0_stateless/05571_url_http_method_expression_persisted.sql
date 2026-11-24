-- Tags: no-replicated-database
-- no-replicated-database: DETACH TABLE without PERMANENTLY.

-- An `http_method` expression of the URL engine is evaluated once, when the table is created: the stored definition
-- keeps the evaluated method, so a reload under other settings neither fails nor reads with another method.

DROP TABLE IF EXISTS url_method_expr;

SET custom_http_method = 'POST';
CREATE TABLE url_method_expr (x UInt8) ENGINE = URL('http://localhost:1/data.csv', CSV, http_method = getSetting('custom_http_method'));

SET custom_http_method = '';
DETACH TABLE url_method_expr;
ATTACH TABLE url_method_expr;
SHOW CREATE TABLE url_method_expr FORMAT TSVRaw;

DROP TABLE url_method_expr;
