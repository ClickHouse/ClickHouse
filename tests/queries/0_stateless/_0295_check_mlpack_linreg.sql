CREATE TABLE testlinreg1 (x1 Float64, x2 Float64, x3 Float64, y Float64) engine=LinReg('testlinreg1.xml') settings lambda=0.0;
INSERT INTO testlinreg1 (*) values (1, 1, 1, 1);
create table checklinregansw1 (x1 Float64, x2 Float64, x3 Float64) engine=Memory;
INSERT INTO checklinregansw1 (*) values (1, 1, 1);
SELECT modelEvaluate('mlpack testlinreg1', x1, x2, x3) FROM checklinregansw1
