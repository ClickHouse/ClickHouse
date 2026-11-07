-- Engine: Spark 3.5.1 fd86f85e181fc2dc0f50a096855acf83a6cc5d9c
CREATE DATABASE IF NOT EXISTS mydb;

CREATE TABLE mydb.multiple_branches (a int, b int) USING iceberg;

INSERT INTO
    mydb.multiple_branches
VALUES
    (1, 1),
    (2, 1),
    (3, 2);

ALTER TABLE
    mydb.multiple_branches CREATE BRANCH `new_branch`;

ALTER TABLE
    mydb.multiple_branches DROP COLUMN b;

INSERT INTO
    mydb.multiple_branches
VALUES
    (4),
    (5),
    (6);
