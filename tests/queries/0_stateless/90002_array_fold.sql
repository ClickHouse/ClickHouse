SELECT arrayFold(x,acc -> acc+x, range(number), toInt64(0)) FROM system.numbers LIMIT 10;
SELECT arrayFold(x,acc -> acc+x, range(number), number) FROM system.numbers LIMIT 10;
SELECT arrayFold(x,acc -> arrayPushFront(acc, x), range(number), emptyArrayUInt64()) FROM system.numbers LIMIT 10;
SELECT arrayFold(x,acc -> x % 2 ? arrayPushFront(acc, x) : arrayPushBack(acc, x), range(number), emptyArrayUInt64()) FROM system.numbers LIMIT 10;
SELECT arrayFold(x,acc -> (acc.1+x, acc.2-x), range(number), (toInt64(0), toInt64(0))) FROM system.numbers LIMIT 10;
SELECT arrayFold(x,acc -> (acc.1+x.1, acc.2-x.2), arrayZip(range(number), range(number)), (toInt64(0), toInt64(0))) FROM system.numbers LIMIT 10;
SELECT arrayFold(x,acc -> arrayPushFront(acc, (x, x+1)), range(number), [(toUInt64(0),toUInt64(0))]) FROM system.numbers LIMIT 10;
SELECT arrayFold(x, acc -> concat(acc, arrayMap(z -> toString(x), [number])) , range(number), CAST([] as Array(String))) FROM system.numbers LIMIT 10;
