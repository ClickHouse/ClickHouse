select cast(1, 'UInt8') from (select arrayJoin([1, 2]) as a) t1 left join (select 1 as b) t2 on b = ignore('UInt8');
select isConstant('UInt8'), toFixedString('hello', toUInt8(substring('UInt8', 5, 1))) from (select arrayJoin([1, 2]) as a) t1 left join (select 1 as b) t2 on b = ignore('UInt8');
