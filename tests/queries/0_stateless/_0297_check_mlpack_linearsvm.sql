CREATE TABLE testsvm1 (x1 Float64, x2 Float64, x3 Float64, y Float64) engine=LinReg('testsvm1.xml');
INSERT INTO testsvm1 (*) values (1, 1, 1, 1);
create table checksvmansw1 (x1 Float64, x2 Float64, x3 Float64) engine=Memory;
INSERT INTO checksvmansw1 (*) values (1, 1, 1);
SELECT modelEvaluate('mlpack testsvm1', x1, x2, x3) FROM checksvmansw1
