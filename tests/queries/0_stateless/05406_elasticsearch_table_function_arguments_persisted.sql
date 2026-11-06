-- Evaluated positional arguments are saved in the definition, so a reload does not evaluate them again.
SET enable_elasticsearch = 1;
SET log_comment = 'logs';

DROP TABLE IF EXISTS es_arguments;
CREATE TABLE es_arguments AS elasticsearch('http://localhost:1', getSetting('log_comment'));

SET log_comment = '';
DETACH TABLE es_arguments;
ATTACH TABLE es_arguments;

SHOW CREATE TABLE es_arguments FORMAT TSVRaw;

DROP TABLE es_arguments;
