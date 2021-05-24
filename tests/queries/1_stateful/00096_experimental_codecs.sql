DROP TABLE IF EXISTS hits_experimental;

-- It's not allowed to create a table with experimental codecs unless the user turns off the safety switch.
CREATE TABLE hits_experimental (Title String CODEC(Lizard(10))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 36 }

SET allow_experimental_codecs = 1;

-- Lizard

CREATE TABLE hits_experimental (Title String CODEC(Lizard(10))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO hits_experimental SELECT Title FROM test.hits;
SELECT sum(cityHash64(*)) FROM hits_experimental;

-- It's always allowed to attach a table with experimental codecs.
DETACH TABLE hits_experimental;
SET allow_experimental_codecs = 0;
ATTACH TABLE hits_experimental;
SELECT sum(cityHash64(*)) FROM hits_experimental;
SET allow_experimental_codecs = 1;

DROP TABLE hits_experimental;

-- Density

-- Check out of range levels of Density.
CREATE TABLE hits_experimental (Title String CODEC(Density(-1))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 433 }
CREATE TABLE hits_experimental (Title String CODEC(Density(0))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 433 }
CREATE TABLE hits_experimental (Title String CODEC(Density(4))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 433 }
CREATE TABLE hits_experimental (Title String CODEC(Density('hello'))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 433 }

CREATE TABLE hits_experimental (Title String CODEC(Density(1))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO hits_experimental SELECT Title FROM test.hits;
SELECT sum(cityHash64(*)) FROM hits_experimental;
DROP TABLE hits_experimental;

CREATE TABLE hits_experimental (Title String CODEC(Density(2))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO hits_experimental SELECT Title FROM test.hits;
SELECT sum(cityHash64(*)) FROM hits_experimental;
DROP TABLE hits_experimental;

CREATE TABLE hits_experimental (Title String CODEC(Density(3))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO hits_experimental SELECT Title FROM test.hits;
SELECT sum(cityHash64(*)) FROM hits_experimental;
DROP TABLE hits_experimental;

-- LZSSE

CREATE TABLE hits_experimental (Title String CODEC(LZSSE2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO hits_experimental SELECT Title FROM test.hits ORDER BY Title LIMIT 100000;
SELECT sum(cityHash64(*)) FROM hits_experimental;
DROP TABLE hits_experimental;

CREATE TABLE hits_experimental (Title String CODEC(LZSSE4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO hits_experimental SELECT Title FROM test.hits ORDER BY Title LIMIT 100000;
SELECT sum(cityHash64(*)) FROM hits_experimental;
DROP TABLE hits_experimental;

CREATE TABLE hits_experimental (Title String CODEC(LZSSE8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO hits_experimental SELECT Title FROM test.hits ORDER BY Title LIMIT 100000;
SELECT sum(cityHash64(*)) FROM hits_experimental;
DROP TABLE hits_experimental;
