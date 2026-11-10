-- The inverse dictionary lookup probes the attribute `DEFAULT` against the same set as `IN` builds,
-- including the text rendering of `true` under `bool_true_representation`.

SET bool_true_representation = 'yes';

DROP DICTIONARY IF EXISTS d_inverse_lookup_bool;
CREATE DICTIONARY d_inverse_lookup_bool (id UInt64, v String DEFAULT 'yes')
PRIMARY KEY id SOURCE(NULL()) LAYOUT(HASHED()) LIFETIME(0);

SELECT number, dictGetString('d_inverse_lookup_bool', 'v', number) IN (true) FROM numbers(3) ORDER BY number SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT number, dictGetString('d_inverse_lookup_bool', 'v', number) IN (true) FROM numbers(3) ORDER BY number SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT number, dictGetString('d_inverse_lookup_bool', 'v', number) NOT IN (true) FROM numbers(3) ORDER BY number SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT number, dictGetString('d_inverse_lookup_bool', 'v', number) NOT IN (true) FROM numbers(3) ORDER BY number SETTINGS optimize_inverse_dictionary_lookup = 1;

DROP DICTIONARY d_inverse_lookup_bool;
