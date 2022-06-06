CREATE TABLE testlogreg1 (x1 Float64, x2 Float64, x3 Float64, y Float64) engine=LinReg('testlogreg1.xml');
INSERT INTO testlogreg1 (*) values (1, 1, 1, 1);
create table checklogregansw1 (x1 Float64, x2 Float64, x3 Float64) engine=Memory;
INSERT INTO checklogregansw1 (*) values (1, 1, 1);
SELECT modelEvaluate('mlpack testlogreg1', x1, x2, x3) FROM checklogregansw1
