-- Engine: Spark 3.5.1 fd86f85e181fc2dc0f50a096855acf83a6cc5d9c
CREATE TABLE prod.db.refdeletes3 (a int, b int) USING iceberg TBLPROPERTIES(
    'write.delete.mode' = 'merge-on-read',
    'write.delete.granularity' = 'file',
    'write.target-file-size-bytes' = 200
);

INSERT INTO
    prod.db.refdeletes3
VALUES
    (0, 12),
    (1, 123),
    (2, 2314),
    (3, 9),
    (4, 1292),
    (5, 12831),
    (6, 12381),
    (7, 123999),
    (8, 12318231),
    (9, 999),
    (10, 1010),
    (11, 11),
    (12, 1212),
    (13, 1313),
    (14, 91121),
    (15, 182222);

DELETE FROM
    prod.db.refdeletes3
WHERE
    a % 3 == 0;
