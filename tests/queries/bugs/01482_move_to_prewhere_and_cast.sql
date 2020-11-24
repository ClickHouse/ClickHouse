DROP TABLE IF EXISTS APPLICATION;
DROP TABLE IF EXISTS DATABASE_IO;

CREATE TABLE APPLICATION (
  `Name` LowCardinality(String),
  `Base` LowCardinality(String)
) ENGINE = Memory();

insert into table APPLICATION values ('ApplicationA', 'BaseA'), ('ApplicationB', 'BaseB') , ('ApplicationC', 'BaseC');

CREATE TABLE DATABASE_IO (
  `Application` LowCardinality(String),
  `Base` LowCardinality(String),
  `Date` DateTime,
  `Ios` UInt32  ) 
ENGINE = MergeTree()
ORDER BY Date;
  
insert into table DATABASE_IO  values ('AppA', 'BaseA', '2020-01-01 00:00:00', 1000);

SELECT `APPLICATION`.`Name` AS `App`,
       CAST(CAST(`DATABASE_IO`.`Date` AS DATE) AS DATE) AS `date` 
FROM   `DATABASE_IO`
INNER 
JOIN   `APPLICATION` ON (`DATABASE_IO`.`Base` = `APPLICATION`.`Base`)
WHERE (
       CAST(CAST(`DATABASE_IO`.`Date` AS DATE) AS TIMESTAMP) >= toDateTime('2020-01-01 00:00:00')
);

DROP TABLE APPLICATION;
DROP TABLE DATABASE_IO;
