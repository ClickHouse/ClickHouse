-- CREATE HYPOTHETICAL INDEX rejects what ALTER TABLE ADD INDEX rejects, with the same error

DROP TABLE IF EXISTS t_hyp_checks;
CREATE TABLE t_hyp_checks (k UInt64, j JSON, s String, INDEX real_idx s TYPE bloom_filter) ENGINE = MergeTree ORDER BY k;

SET allow_minmax_index_for_json = 0;
ALTER TABLE t_hyp_checks ADD INDEX i_json j TYPE minmax; -- { serverError BAD_ARGUMENTS }
CREATE HYPOTHETICAL INDEX i_json ON t_hyp_checks (j) TYPE minmax; -- { serverError BAD_ARGUMENTS }

ALTER TABLE t_hyp_checks ADD INDEX real_idx k TYPE minmax; -- { serverError ILLEGAL_COLUMN }
CREATE HYPOTHETICAL INDEX real_idx ON t_hyp_checks (k) TYPE minmax; -- { serverError ILLEGAL_COLUMN }

SET allow_suspicious_indexes = 0;
ALTER TABLE t_hyp_checks ADD INDEX i_dup (k, k) TYPE minmax; -- { serverError BAD_ARGUMENTS }
CREATE HYPOTHETICAL INDEX i_dup ON t_hyp_checks (k, k) TYPE minmax; -- { serverError BAD_ARGUMENTS }

-- the setting that allows the real index allows the hypothetical one
SET allow_minmax_index_for_json = 1;
CREATE HYPOTHETICAL INDEX i_json ON t_hyp_checks (j) TYPE minmax;
SELECT name FROM system.hypothetical_indexes WHERE table = 't_hyp_checks' AND database = currentDatabase();

DROP TABLE t_hyp_checks;
