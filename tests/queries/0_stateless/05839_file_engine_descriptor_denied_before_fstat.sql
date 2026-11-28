-- The `File` table engine with a file descriptor is not allowed in the server.
-- The error must be the same whether the descriptor is open or not:
-- otherwise a query could find out which descriptor numbers the server uses.

CREATE TABLE t_file_descriptor (x String) ENGINE = File(TSV, 0); -- { serverError DATABASE_ACCESS_DENIED }
CREATE TABLE t_file_descriptor (x String) ENGINE = File(TSV, stdin); -- { serverError DATABASE_ACCESS_DENIED }

-- A descriptor number that is not open in the server.
CREATE TABLE t_file_descriptor (x String) ENGINE = File(TSV, 1000000); -- { serverError DATABASE_ACCESS_DENIED }
CREATE TEMPORARY TABLE t_file_descriptor (x String) ENGINE = File(TSV, 1000000); -- { serverError DATABASE_ACCESS_DENIED }
