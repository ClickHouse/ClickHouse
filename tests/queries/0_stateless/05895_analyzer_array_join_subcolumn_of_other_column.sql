SELECT x.y FROM (SELECT CAST(tuple(7), 'Tuple(y UInt8)') AS x, CAST([tuple(9)], 'Array(Tuple(y UInt8))') AS z) ARRAY JOIN z;
SELECT x.y, z.y FROM (SELECT CAST(tuple(7), 'Tuple(y UInt8)') AS x, CAST([tuple(9)], 'Array(Tuple(y UInt8))') AS z) ARRAY JOIN z;
SELECT s.x.y, s.z.y FROM (SELECT CAST(tuple(7), 'Tuple(y UInt8)') AS x, CAST([tuple(9)], 'Array(Tuple(y UInt8))') AS z) AS s ARRAY JOIN z;
SELECT x.y, z.y FROM (SELECT CAST(tuple(7), 'Tuple(y UInt8)') AS x, CAST([tuple(9)], 'Array(Tuple(y UInt8))') AS z) LEFT ARRAY JOIN z;
SELECT c.pe.a, d.pe.a FROM (SELECT CAST(tuple([tuple(1), tuple(2)]), 'Tuple(pe Array(Tuple(a UInt8)))') AS c, CAST(tuple([tuple(5), tuple(6)]), 'Tuple(pe Array(Tuple(a UInt8)))') AS d) ARRAY JOIN c.pe ORDER BY 1;
