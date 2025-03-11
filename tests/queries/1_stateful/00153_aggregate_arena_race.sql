-- Tags: race

create temporary table dest00153 (`s` AggregateFunction(groupUniqArray, String)) engine Memory;
insert into dest00153 select groupUniqArrayState(RefererDomain) from test.hits group by URLDomain;
