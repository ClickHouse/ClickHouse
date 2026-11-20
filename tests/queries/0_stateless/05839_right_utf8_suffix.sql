WITH 'aaa' || char(160) || 'bbb' AS s
SELECT hex(rightUTF8(s, 9)), hex(rightUTF8(materialize(s), 9)), hex(rightUTF8(materialize(s), materialize(9))), hex(rightUTF8(s, materialize(3)));
SELECT rightUTF8('aé界', materialize(2)), rightUTF8('aé界', materialize(-1)), empty(rightUTF8('aé界', materialize(0)));
