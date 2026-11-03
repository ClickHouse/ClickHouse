-- Tags: no-fasttest
-- no-fasttest: the MongoDB table function, table engine and dictionary source need USE_MONGODB

-- While `s3_allow_server_credentials_in_user_queries` is disabled, a MongoDB connection string may not make the server
-- authenticate with credentials of the server process: an authentication mechanism that takes them from its
-- environment, or a client certificate file from its filesystem. No MongoDB server is contacted: the refused queries
-- fail before connecting, and the accepted ones only describe the table function.

SET describe_compact_output = 1;
SET s3_allow_server_credentials_in_user_queries = 0;

DESCRIBE mongodb('mongodb://127.0.0.1:1/db?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s', 'c', 'x UInt8'); -- { serverError ACCESS_DENIED }
SELECT count() FROM mongodb('mongodb://127.0.0.1:1/db?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s', 'c', 'x UInt8'); -- { serverError ACCESS_DENIED }
DESCRIBE mongodb('mongodb://127.0.0.1:1/db?authmechanism=mongodb-oidc', 'c', 'x UInt8'); -- { serverError ACCESS_DENIED }
DESCRIBE mongodb('mongodb://127.0.0.1:1/db?authMechanism=MONGODB-AWS', 'c', 'x UInt8'); -- { serverError ACCESS_DENIED }
DESCRIBE mongodb('mongodb://user@127.0.0.1:1/db?authMechanism=GSSAPI', 'c', 'x UInt8'); -- { serverError ACCESS_DENIED }
DESCRIBE mongodb('mongodb://127.0.0.1:1/db?tls=true&tlsCertificateKeyFile=/nonexistent.pem', 'c', 'x UInt8'); -- { serverError ACCESS_DENIED }
DESCRIBE mongodb('mongodb://127.0.0.1:1/db?ssl=true&sslClientCertificateKeyFile=/nonexistent.pem', 'c', 'x UInt8'); -- { serverError ACCESS_DENIED }
DESCRIBE mongodb('127.0.0.1:1', 'db', 'c', '', '', 'x UInt8', 'authMechanism=MONGODB-OIDC'); -- { serverError ACCESS_DENIED }

DROP TABLE IF EXISTS t_05496;
CREATE TABLE t_05496 (x UInt8) ENGINE = MongoDB('mongodb://127.0.0.1:1/db?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s', 'c'); -- { serverError ACCESS_DENIED }
CREATE TABLE t_05496 (x UInt8) ENGINE = MongoDB('127.0.0.1:1', 'db', 'c', '', '', 'tlsCertificateKeyFile=/nonexistent.pem'); -- { serverError ACCESS_DENIED }

-- Mechanisms that take the credentials from the connection string are accepted.
DESCRIBE mongodb('mongodb://user:pass@127.0.0.1:1/db', 'c', 'x UInt8');
DESCRIBE mongodb('mongodb://user:pass@127.0.0.1:1/db?authMechanism=SCRAM-SHA-256', 'c', 'x UInt8');
DESCRIBE mongodb('mongodb://user:pass@127.0.0.1:1/db?authMechanism=plain', 'c', 'x UInt8');
DESCRIBE mongodb('mongodb://127.0.0.1:1/db?authMechanism=MONGODB-X509', 'c', 'x UInt8');

-- The setting permits the others.
SET s3_allow_server_credentials_in_user_queries = 1;
DESCRIBE mongodb('mongodb://127.0.0.1:1/db?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s', 'c', 'x UInt8');
DESCRIBE mongodb('mongodb://127.0.0.1:1/db?tls=true&tlsCertificateKeyFile=/nonexistent.pem', 'c', 'x UInt8');

-- A table created while the setting is enabled is refused to a session where it is disabled. Loading the table again
-- from its stored definition does not check it, so that succeeds, and the table stays refused.
CREATE TABLE t_05496 (x UInt8) ENGINE = MongoDB('mongodb://127.0.0.1:1/db?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s', 'c');
SET s3_allow_server_credentials_in_user_queries = 0;
SELECT * FROM t_05496; -- { serverError ACCESS_DENIED }
DETACH TABLE t_05496;
ATTACH TABLE t_05496;
SELECT count() FROM t_05496; -- { serverError ACCESS_DENIED }
DROP TABLE t_05496;

-- A dictionary is loaded by the server, so the setting of the session that creates or queries it does not apply.
SET s3_allow_server_credentials_in_user_queries = 1;
DROP DICTIONARY IF EXISTS d_05496;
CREATE DICTIONARY d_05496 (_id String, v String) PRIMARY KEY _id
SOURCE(MONGODB(URI 'mongodb://127.0.0.1:1/db?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s' COLLECTION 'c'))
LAYOUT(COMPLEX_KEY_DIRECT());
SELECT dictGet('d_05496', 'v', tuple('k')); -- { serverError ACCESS_DENIED }
DROP DICTIONARY d_05496;
