# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.6.210226.1200017.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Specification
from testflows.core import Requirement

Heading = Specification.Heading

RQ_SRS_018_ClickHouse_Map_DataType = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `Map(key, value)` data type that stores `key:value` pairs.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.1.1')

RQ_SRS_018_ClickHouse_Map_DataType_Performance_Vs_ArrayOfTuples = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Performance.Vs.ArrayOfTuples',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL provide comparable performance for `Map(key, value)` data type as\n'
        'compared to `Array(Tuple(K,V))` data type.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.2.1')

RQ_SRS_018_ClickHouse_Map_DataType_Performance_Vs_TupleOfArrays = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Performance.Vs.TupleOfArrays',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL provide comparable performance for `Map(key, value)` data type as\n'
        'compared to `Tuple(Array(String), Array(String))` data type where the first\n'
        'array defines an array of keys and the second array defines an array of values.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.2.2')

RQ_SRS_018_ClickHouse_Map_DataType_Key_String = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Key.String',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `Map(key, value)` data type where key is of a [String] type.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.3.1')

RQ_SRS_018_ClickHouse_Map_DataType_Key_Integer = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Key.Integer',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `Map(key, value)` data type where key is of an [Integer] type.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.3.2')

RQ_SRS_018_ClickHouse_Map_DataType_Value_String = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Value.String',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `Map(key, value)` data type where value is of a [String] type.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.4.1')

RQ_SRS_018_ClickHouse_Map_DataType_Value_Integer = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Value.Integer',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `Map(key, value)` data type where value is of a [Integer] type.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.4.2')

RQ_SRS_018_ClickHouse_Map_DataType_Value_Array = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Value.Array',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `Map(key, value)` data type where value is of a [Array] type.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.4.3')

RQ_SRS_018_ClickHouse_Map_DataType_Invalid_Nullable = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Invalid.Nullable',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL not support creating table columns that have `Nullable(Map(key, value))` data type.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.5.1')

RQ_SRS_018_ClickHouse_Map_DataType_Invalid_NothingNothing = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Invalid.NothingNothing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL not support creating table columns that have `Map(Nothing, Nothing))` data type.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.5.2')

RQ_SRS_018_ClickHouse_Map_DataType_DuplicatedKeys = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.DuplicatedKeys',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] MAY support `Map(key, value)` data type with duplicated keys.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.6.1')

RQ_SRS_018_ClickHouse_Map_DataType_ArrayOfMaps = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.ArrayOfMaps',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `Array(Map(key, value))` data type.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.7.1')

RQ_SRS_018_ClickHouse_Map_DataType_NestedWithMaps = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.NestedWithMaps',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support defining `Map(key, value)` data type inside the [Nested] data type.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.8.1')

RQ_SRS_018_ClickHouse_Map_DataType_Value_Retrieval = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Value.Retrieval',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support getting the value from a `Map(key, value)` data type using `map[key]` syntax.\n'
        'If `key` has duplicates then the first `key:value` pair MAY be returned. \n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT a['key2'] FROM table_map;\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.9.1')

RQ_SRS_018_ClickHouse_Map_DataType_Value_Retrieval_KeyInvalid = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Value.Retrieval.KeyInvalid',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when key does not match the key type.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT map(1,2) AS m, m[1024]\n'
        '```\n'
        '\n'
        'Exceptions:\n'
        '\n'
        '* when key is `NULL` the return value MAY be `NULL`\n'
        '* when key value is not valid for the key type, for example it is out of range for [Integer] type, \n'
        '  when reading from a table column it MAY return the default value for key data type\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.9.2')

RQ_SRS_018_ClickHouse_Map_DataType_Value_Retrieval_KeyNotFound = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Value.Retrieval.KeyNotFound',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return default value for the data type of the value\n'
        "when there's no corresponding `key` defined in the `Map(key, value)` data type. \n"
        '\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.9.3')

RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_TupleOfArraysToMap = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.TupleOfArraysToMap',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support converting [Tuple(Array, Array)] to `Map(key, value)` using the [CAST] function.\n'
        '\n'
        '``` sql\n'
        "SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;\n"
        '```\n'
        '\n'
        '``` text\n'
        '┌─map───────────────────────────┐\n'
        "│ {1:'Ready',2:'Steady',3:'Go'} │\n"
        '└───────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.10.1')

RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_TupleOfArraysMap_Invalid = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.TupleOfArraysMap.Invalid',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] MAY return an error when casting [Tuple(Array, Array)] to `Map(key, value)`\n'
        '\n'
        '* when arrays are not of equal size\n'
        '\n'
        '  For example,\n'
        '\n'
        '  ```sql\n'
        "  SELECT CAST(([2, 1, 1023], ['', '']), 'Map(UInt8, String)') AS map, map[10]\n"
        '  ```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.10.2')

RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_ArrayOfTuplesToMap = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.ArrayOfTuplesToMap',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support converting [Array(Tuple(K,V))] to `Map(key, value)` using the [CAST] function.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT CAST(([(1,2),(3)]), 'Map(UInt8, UInt8)') AS map\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.11.1')

RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_ArrayOfTuplesToMap_Invalid = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.ArrayOfTuplesToMap.Invalid',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] MAY return an error when casting [Array(Tuple(K, V))] to `Map(key, value)`\n'
        '\n'
        '* when element is not a [Tuple]\n'
        '\n'
        '  ```sql\n'
        "  SELECT CAST(([(1,2),(3)]), 'Map(UInt8, UInt8)') AS map\n"
        '  ```\n'
        '\n'
        '* when [Tuple] does not contain two elements\n'
        '\n'
        '  ```sql\n'
        "  SELECT CAST(([(1,2),(3,)]), 'Map(UInt8, UInt8)') AS map\n"
        '  ```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.11.2')

RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Keys = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Keys',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `keys` subcolumn in the `Map(key, value)` type that can be used \n'
        'to retrieve an [Array] of map keys.\n'
        '\n'
        '```sql\n'
        'SELECT m.keys FROM t_map;\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.12.1')

RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Keys_ArrayFunctions = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Keys.ArrayFunctions',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support applying [Array] functions to the `keys` subcolumn in the `Map(key, value)` type.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT * FROM t_map WHERE has(m.keys, 'a');\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.12.2')

RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Keys_InlineDefinedMap = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Keys.InlineDefinedMap',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] MAY not support using inline defined map to get `keys` subcolumn.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT map( 'aa', 4, '44' , 5) as c, c.keys\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.12.3')

RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Values = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Values',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `values` subcolumn in the `Map(key, value)` type that can be used \n'
        'to retrieve an [Array] of map values.\n'
        '\n'
        '```sql\n'
        'SELECT m.values FROM t_map;\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.12.4')

RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Values_ArrayFunctions = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Values.ArrayFunctions',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support applying [Array] functions to the `values` subcolumn in the `Map(key, value)` type.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT * FROM t_map WHERE has(m.values, 'a');\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.12.5')

RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Values_InlineDefinedMap = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Values.InlineDefinedMap',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] MAY not support using inline defined map to get `values` subcolumn.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT map( 'aa', 4, '44' , 5) as c, c.values\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.12.6')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_InlineDefinedMap = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.InlineDefinedMap',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support using inline defined maps as an argument to map functions.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT map( 'aa', 4, '44' , 5) as c, mapKeys(c)\n"
        "SELECT map( 'aa', 4, '44' , 5) as c, mapValues(c)\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.13.1')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_Length = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Length',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `Map(key, value)` data type as an argument to the [length] function\n'
        'that SHALL return number of keys in the map.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT length(map(1,2,3,4))\n'
        'SELECT length(map())\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.13.2.1')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_Empty = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Empty',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `Map(key, value)` data type as an argument to the [empty] function\n'
        'that SHALL return 1 if number of keys in the map is 0 otherwise if the number of keys is \n'
        'greater or equal to 1 it SHALL return 0.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT empty(map(1,2,3,4))\n'
        'SELECT empty(map())\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.13.3.1')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_NotEmpty = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.NotEmpty',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `Map(key, value)` data type as an argument to the [notEmpty] function\n'
        'that SHALL return 0 if number if keys in the map is 0 otherwise if the number of keys is\n'
        'greater or equal to 1 it SHALL return 1.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT notEmpty(map(1,2,3,4))\n'
        'SELECT notEmpty(map())\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.13.4.1')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support arranging `key, value` pairs into `Map(key, value)` data type\n'
        'using `map` function.\n'
        '\n'
        '**Syntax** \n'
        '\n'
        '``` sql\n'
        'map(key1, value1[, key2, value2, ...])\n'
        '```\n'
        '\n'
        'For example,\n'
        '\n'
        '``` sql\n'
        "SELECT map('key1', number, 'key2', number * 2) FROM numbers(3);\n"
        '\n'
        "┌─map('key1', number, 'key2', multiply(number, 2))─┐\n"
        "│ {'key1':0,'key2':0}                              │\n"
        "│ {'key1':1,'key2':2}                              │\n"
        "│ {'key1':2,'key2':4}                              │\n"
        '└──────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.13.5.1')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_InvalidNumberOfArguments = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.InvalidNumberOfArguments',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `map` function is called with non even number of arguments.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.13.5.2')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_MixedKeyOrValueTypes = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MixedKeyOrValueTypes',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `map` function is called with mixed key or value types.\n'
        '\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.13.5.3')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_MapAdd = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MapAdd',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support converting the results of `mapAdd` function to a `Map(key, value)` data type.\n'
        '\n'
        'For example,\n'
        '\n'
        '``` sql\n'
        'SELECT CAST(mapAdd(([toUInt8(1), 2], [1, 1]), ([toUInt8(1), 2], [1, 1])), "Map(Int8,Int8)")\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.13.5.4')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_MapSubstract = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MapSubstract',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support converting the results of `mapSubstract` function to a `Map(key, value)` data type.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT CAST(mapSubtract(([toUInt8(1), 2], [toInt32(1), 1]), ([toUInt8(1), 2], [toInt32(2), 1])), "Map(Int8,Int8)")\n'
        '```\n'
        ),
    link=None,
    level=4,
    num='3.13.5.5')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_MapPopulateSeries = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MapPopulateSeries',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support converting the results of `mapPopulateSeries` function to a `Map(key, value)` data type.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT CAST(mapPopulateSeries([1,2,4], [11,22,44], 5), "Map(Int8,Int8)")\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.13.5.6')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_MapContains = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.MapContains',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `mapContains(map, key)` function to check weather `map.keys` contains the `key`.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT mapContains(a, 'abc') from table_map;\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.13.6.1')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_MapKeys = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.MapKeys',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `mapKeys(map)` function to return all the map keys in the [Array] format.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT mapKeys(a) from table_map;\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.13.7.1')

RQ_SRS_018_ClickHouse_Map_DataType_Functions_MapValues = Requirement(
    name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.MapValues',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `mapValues(map)` function to return all the map values in the [Array] format.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT mapValues(a) from table_map;\n'
        '```\n'
        '\n'
        '[Nested]: https://clickhouse.tech/docs/en/sql-reference/data-types/nested-data-structures/nested/\n'
        '[length]: https://clickhouse.tech/docs/en/sql-reference/functions/array-functions/#array_functions-length\n'
        '[empty]: https://clickhouse.tech/docs/en/sql-reference/functions/array-functions/#function-empty\n'
        '[notEmpty]: https://clickhouse.tech/docs/en/sql-reference/functions/array-functions/#function-notempty\n'
        '[CAST]: https://clickhouse.tech/docs/en/sql-reference/functions/type-conversion-functions/#type_conversion_function-cast\n'
        '[Tuple]: https://clickhouse.tech/docs/en/sql-reference/data-types/tuple/\n'
        '[Tuple(Array,Array)]: https://clickhouse.tech/docs/en/sql-reference/data-types/tuple/\n'
        '[Array]: https://clickhouse.tech/docs/en/sql-reference/data-types/array/ \n'
        '[String]: https://clickhouse.tech/docs/en/sql-reference/data-types/string/\n'
        '[Integer]: https://clickhouse.tech/docs/en/sql-reference/data-types/int-uint/\n'
        '[ClickHouse]: https://clickhouse.tech\n'
        '[GitHub Repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/map_type/requirements/requirements.md \n'
        '[Revision History]: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/map_type/requirements/requirements.md\n'
        '[Git]: https://git-scm.com/\n'
        '[GitHub]: https://github.com\n'
        ),
    link=None,
    level=4,
    num='3.13.8.1')

SRS018_ClickHouse_Map_Data_Type = Specification(
    name='SRS018 ClickHouse Map Data Type', 
    description=None,
    author=None,
    date=None, 
    status=None, 
    approved_by=None,
    approved_date=None,
    approved_version=None,
    version=None,
    group=None,
    type=None,
    link=None,
    uid=None,
    parent=None,
    children=None,
    headings=(
        Heading(name='Revision History', level=1, num='1'),
        Heading(name='Introduction', level=1, num='2'),
        Heading(name='Requirements', level=1, num='3'),
        Heading(name='General', level=2, num='3.1'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType', level=3, num='3.1.1'),
        Heading(name='Performance', level=2, num='3.2'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Performance.Vs.ArrayOfTuples', level=3, num='3.2.1'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Performance.Vs.TupleOfArrays', level=3, num='3.2.2'),
        Heading(name='Key Types', level=2, num='3.3'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Key.String', level=3, num='3.3.1'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Key.Integer', level=3, num='3.3.2'),
        Heading(name='Value Types', level=2, num='3.4'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Value.String', level=3, num='3.4.1'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Value.Integer', level=3, num='3.4.2'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Value.Array', level=3, num='3.4.3'),
        Heading(name='Invalid Types', level=2, num='3.5'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Invalid.Nullable', level=3, num='3.5.1'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Invalid.NothingNothing', level=3, num='3.5.2'),
        Heading(name='Duplicated Keys', level=2, num='3.6'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.DuplicatedKeys', level=3, num='3.6.1'),
        Heading(name='Array of Maps', level=2, num='3.7'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.ArrayOfMaps', level=3, num='3.7.1'),
        Heading(name='Nested With Maps', level=2, num='3.8'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.NestedWithMaps', level=3, num='3.8.1'),
        Heading(name='Value Retrieval', level=2, num='3.9'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Value.Retrieval', level=3, num='3.9.1'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Value.Retrieval.KeyInvalid', level=3, num='3.9.2'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Value.Retrieval.KeyNotFound', level=3, num='3.9.3'),
        Heading(name='Converting Tuple(Array, Array) to Map', level=2, num='3.10'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.TupleOfArraysToMap', level=3, num='3.10.1'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.TupleOfArraysMap.Invalid', level=3, num='3.10.2'),
        Heading(name='Converting Array(Tuple(K,V)) to Map', level=2, num='3.11'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.ArrayOfTuplesToMap', level=3, num='3.11.1'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.ArrayOfTuplesToMap.Invalid', level=3, num='3.11.2'),
        Heading(name='Keys and Values Subcolumns', level=2, num='3.12'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Keys', level=3, num='3.12.1'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Keys.ArrayFunctions', level=3, num='3.12.2'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Keys.InlineDefinedMap', level=3, num='3.12.3'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Values', level=3, num='3.12.4'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Values.ArrayFunctions', level=3, num='3.12.5'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Values.InlineDefinedMap', level=3, num='3.12.6'),
        Heading(name='Functions', level=2, num='3.13'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.InlineDefinedMap', level=3, num='3.13.1'),
        Heading(name='`length`', level=3, num='3.13.2'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Length', level=4, num='3.13.2.1'),
        Heading(name='`empty`', level=3, num='3.13.3'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Empty', level=4, num='3.13.3.1'),
        Heading(name='`notEmpty`', level=3, num='3.13.4'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.NotEmpty', level=4, num='3.13.4.1'),
        Heading(name='`map`', level=3, num='3.13.5'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map', level=4, num='3.13.5.1'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.InvalidNumberOfArguments', level=4, num='3.13.5.2'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MixedKeyOrValueTypes', level=4, num='3.13.5.3'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MapAdd', level=4, num='3.13.5.4'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MapSubstract', level=4, num='3.13.5.5'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MapPopulateSeries', level=4, num='3.13.5.6'),
        Heading(name='`mapContains`', level=3, num='3.13.6'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.MapContains', level=4, num='3.13.6.1'),
        Heading(name='`mapKeys`', level=3, num='3.13.7'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.MapKeys', level=4, num='3.13.7.1'),
        Heading(name='`mapValues`', level=3, num='3.13.8'),
        Heading(name='RQ.SRS-018.ClickHouse.Map.DataType.Functions.MapValues', level=4, num='3.13.8.1'),
        ),
    requirements=(
        RQ_SRS_018_ClickHouse_Map_DataType,
        RQ_SRS_018_ClickHouse_Map_DataType_Performance_Vs_ArrayOfTuples,
        RQ_SRS_018_ClickHouse_Map_DataType_Performance_Vs_TupleOfArrays,
        RQ_SRS_018_ClickHouse_Map_DataType_Key_String,
        RQ_SRS_018_ClickHouse_Map_DataType_Key_Integer,
        RQ_SRS_018_ClickHouse_Map_DataType_Value_String,
        RQ_SRS_018_ClickHouse_Map_DataType_Value_Integer,
        RQ_SRS_018_ClickHouse_Map_DataType_Value_Array,
        RQ_SRS_018_ClickHouse_Map_DataType_Invalid_Nullable,
        RQ_SRS_018_ClickHouse_Map_DataType_Invalid_NothingNothing,
        RQ_SRS_018_ClickHouse_Map_DataType_DuplicatedKeys,
        RQ_SRS_018_ClickHouse_Map_DataType_ArrayOfMaps,
        RQ_SRS_018_ClickHouse_Map_DataType_NestedWithMaps,
        RQ_SRS_018_ClickHouse_Map_DataType_Value_Retrieval,
        RQ_SRS_018_ClickHouse_Map_DataType_Value_Retrieval_KeyInvalid,
        RQ_SRS_018_ClickHouse_Map_DataType_Value_Retrieval_KeyNotFound,
        RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_TupleOfArraysToMap,
        RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_TupleOfArraysMap_Invalid,
        RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_ArrayOfTuplesToMap,
        RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_ArrayOfTuplesToMap_Invalid,
        RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Keys,
        RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Keys_ArrayFunctions,
        RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Keys_InlineDefinedMap,
        RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Values,
        RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Values_ArrayFunctions,
        RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Values_InlineDefinedMap,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_InlineDefinedMap,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_Length,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_Empty,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_NotEmpty,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_InvalidNumberOfArguments,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_MixedKeyOrValueTypes,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_MapAdd,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_MapSubstract,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_MapPopulateSeries,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_MapContains,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_MapKeys,
        RQ_SRS_018_ClickHouse_Map_DataType_Functions_MapValues,
        ),
    content='''
# SRS018 ClickHouse Map Data Type
# Software Requirements Specification

## Table of Contents

* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
* 3 [Requirements](#requirements)
  * 3.1 [General](#general)
    * 3.1.1 [RQ.SRS-018.ClickHouse.Map.DataType](#rqsrs-018clickhousemapdatatype)
  * 3.2 [Performance](#performance)
    * 3.2.1 [RQ.SRS-018.ClickHouse.Map.DataType.Performance.Vs.ArrayOfTuples](#rqsrs-018clickhousemapdatatypeperformancevsarrayoftuples)
    * 3.2.2 [RQ.SRS-018.ClickHouse.Map.DataType.Performance.Vs.TupleOfArrays](#rqsrs-018clickhousemapdatatypeperformancevstupleofarrays)
  * 3.3 [Key Types](#key-types)
    * 3.3.1 [RQ.SRS-018.ClickHouse.Map.DataType.Key.String](#rqsrs-018clickhousemapdatatypekeystring)
    * 3.3.2 [RQ.SRS-018.ClickHouse.Map.DataType.Key.Integer](#rqsrs-018clickhousemapdatatypekeyinteger)
  * 3.4 [Value Types](#value-types)
    * 3.4.1 [RQ.SRS-018.ClickHouse.Map.DataType.Value.String](#rqsrs-018clickhousemapdatatypevaluestring)
    * 3.4.2 [RQ.SRS-018.ClickHouse.Map.DataType.Value.Integer](#rqsrs-018clickhousemapdatatypevalueinteger)
    * 3.4.3 [RQ.SRS-018.ClickHouse.Map.DataType.Value.Array](#rqsrs-018clickhousemapdatatypevaluearray)
  * 3.5 [Invalid Types](#invalid-types)
    * 3.5.1 [RQ.SRS-018.ClickHouse.Map.DataType.Invalid.Nullable](#rqsrs-018clickhousemapdatatypeinvalidnullable)
    * 3.5.2 [RQ.SRS-018.ClickHouse.Map.DataType.Invalid.NothingNothing](#rqsrs-018clickhousemapdatatypeinvalidnothingnothing)
  * 3.6 [Duplicated Keys](#duplicated-keys)
    * 3.6.1 [RQ.SRS-018.ClickHouse.Map.DataType.DuplicatedKeys](#rqsrs-018clickhousemapdatatypeduplicatedkeys)
  * 3.7 [Array of Maps](#array-of-maps)
    * 3.7.1 [RQ.SRS-018.ClickHouse.Map.DataType.ArrayOfMaps](#rqsrs-018clickhousemapdatatypearrayofmaps)
  * 3.8 [Nested With Maps](#nested-with-maps)
    * 3.8.1 [RQ.SRS-018.ClickHouse.Map.DataType.NestedWithMaps](#rqsrs-018clickhousemapdatatypenestedwithmaps)
  * 3.9 [Value Retrieval](#value-retrieval)
    * 3.9.1 [RQ.SRS-018.ClickHouse.Map.DataType.Value.Retrieval](#rqsrs-018clickhousemapdatatypevalueretrieval)
    * 3.9.2 [RQ.SRS-018.ClickHouse.Map.DataType.Value.Retrieval.KeyInvalid](#rqsrs-018clickhousemapdatatypevalueretrievalkeyinvalid)
    * 3.9.3 [RQ.SRS-018.ClickHouse.Map.DataType.Value.Retrieval.KeyNotFound](#rqsrs-018clickhousemapdatatypevalueretrievalkeynotfound)
  * 3.10 [Converting Tuple(Array, Array) to Map](#converting-tuplearray-array-to-map)
    * 3.10.1 [RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.TupleOfArraysToMap](#rqsrs-018clickhousemapdatatypeconversionfromtupleofarraystomap)
    * 3.10.2 [RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.TupleOfArraysMap.Invalid](#rqsrs-018clickhousemapdatatypeconversionfromtupleofarraysmapinvalid)
  * 3.11 [Converting Array(Tuple(K,V)) to Map](#converting-arraytuplekv-to-map)
    * 3.11.1 [RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.ArrayOfTuplesToMap](#rqsrs-018clickhousemapdatatypeconversionfromarrayoftuplestomap)
    * 3.11.2 [RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.ArrayOfTuplesToMap.Invalid](#rqsrs-018clickhousemapdatatypeconversionfromarrayoftuplestomapinvalid)
  * 3.12 [Keys and Values Subcolumns](#keys-and-values-subcolumns)
    * 3.12.1 [RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Keys](#rqsrs-018clickhousemapdatatypesubcolumnskeys)
    * 3.12.2 [RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Keys.ArrayFunctions](#rqsrs-018clickhousemapdatatypesubcolumnskeysarrayfunctions)
    * 3.12.3 [RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Keys.InlineDefinedMap](#rqsrs-018clickhousemapdatatypesubcolumnskeysinlinedefinedmap)
    * 3.12.4 [RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Values](#rqsrs-018clickhousemapdatatypesubcolumnsvalues)
    * 3.12.5 [RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Values.ArrayFunctions](#rqsrs-018clickhousemapdatatypesubcolumnsvaluesarrayfunctions)
    * 3.12.6 [RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Values.InlineDefinedMap](#rqsrs-018clickhousemapdatatypesubcolumnsvaluesinlinedefinedmap)
  * 3.13 [Functions](#functions)
    * 3.13.1 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.InlineDefinedMap](#rqsrs-018clickhousemapdatatypefunctionsinlinedefinedmap)
    * 3.13.2 [`length`](#length)
      * 3.13.2.1 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.Length](#rqsrs-018clickhousemapdatatypefunctionslength)
    * 3.13.3 [`empty`](#empty)
      * 3.13.3.1 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.Empty](#rqsrs-018clickhousemapdatatypefunctionsempty)
    * 3.13.4 [`notEmpty`](#notempty)
      * 3.13.4.1 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.NotEmpty](#rqsrs-018clickhousemapdatatypefunctionsnotempty)
    * 3.13.5 [`map`](#map)
      * 3.13.5.1 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map](#rqsrs-018clickhousemapdatatypefunctionsmap)
      * 3.13.5.2 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.InvalidNumberOfArguments](#rqsrs-018clickhousemapdatatypefunctionsmapinvalidnumberofarguments)
      * 3.13.5.3 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MixedKeyOrValueTypes](#rqsrs-018clickhousemapdatatypefunctionsmapmixedkeyorvaluetypes)
      * 3.13.5.4 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MapAdd](#rqsrs-018clickhousemapdatatypefunctionsmapmapadd)
      * 3.13.5.5 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MapSubstract](#rqsrs-018clickhousemapdatatypefunctionsmapmapsubstract)
      * 3.13.5.6 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MapPopulateSeries](#rqsrs-018clickhousemapdatatypefunctionsmapmappopulateseries)
    * 3.13.6 [`mapContains`](#mapcontains)
      * 3.13.6.1 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.MapContains](#rqsrs-018clickhousemapdatatypefunctionsmapcontains)
    * 3.13.7 [`mapKeys`](#mapkeys)
      * 3.13.7.1 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.MapKeys](#rqsrs-018clickhousemapdatatypefunctionsmapkeys)
    * 3.13.8 [`mapValues`](#mapvalues)
      * 3.13.8.1 [RQ.SRS-018.ClickHouse.Map.DataType.Functions.MapValues](#rqsrs-018clickhousemapdatatypefunctionsmapvalues)

## Revision History

This document is stored in an electronic form using [Git] source control management software
hosted in a [GitHub Repository].
All the updates are tracked using the [Revision History].

## Introduction

This software requirements specification covers requirements for `Map(key, value)` data type in [ClickHouse].

## Requirements

### General

#### RQ.SRS-018.ClickHouse.Map.DataType
version: 1.0

[ClickHouse] SHALL support `Map(key, value)` data type that stores `key:value` pairs.

### Performance

#### RQ.SRS-018.ClickHouse.Map.DataType.Performance.Vs.ArrayOfTuples
version:1.0

[ClickHouse] SHALL provide comparable performance for `Map(key, value)` data type as
compared to `Array(Tuple(K,V))` data type.

#### RQ.SRS-018.ClickHouse.Map.DataType.Performance.Vs.TupleOfArrays
version:1.0

[ClickHouse] SHALL provide comparable performance for `Map(key, value)` data type as
compared to `Tuple(Array(String), Array(String))` data type where the first
array defines an array of keys and the second array defines an array of values.

### Key Types

#### RQ.SRS-018.ClickHouse.Map.DataType.Key.String
version: 1.0

[ClickHouse] SHALL support `Map(key, value)` data type where key is of a [String] type.

#### RQ.SRS-018.ClickHouse.Map.DataType.Key.Integer
version: 1.0

[ClickHouse] SHALL support `Map(key, value)` data type where key is of an [Integer] type.

### Value Types

#### RQ.SRS-018.ClickHouse.Map.DataType.Value.String
version: 1.0

[ClickHouse] SHALL support `Map(key, value)` data type where value is of a [String] type.

#### RQ.SRS-018.ClickHouse.Map.DataType.Value.Integer
version: 1.0

[ClickHouse] SHALL support `Map(key, value)` data type where value is of a [Integer] type.

#### RQ.SRS-018.ClickHouse.Map.DataType.Value.Array
version: 1.0

[ClickHouse] SHALL support `Map(key, value)` data type where value is of a [Array] type.

### Invalid Types

#### RQ.SRS-018.ClickHouse.Map.DataType.Invalid.Nullable
version: 1.0

[ClickHouse] SHALL not support creating table columns that have `Nullable(Map(key, value))` data type.

#### RQ.SRS-018.ClickHouse.Map.DataType.Invalid.NothingNothing
version: 1.0

[ClickHouse] SHALL not support creating table columns that have `Map(Nothing, Nothing))` data type.

### Duplicated Keys

#### RQ.SRS-018.ClickHouse.Map.DataType.DuplicatedKeys
version: 1.0

[ClickHouse] MAY support `Map(key, value)` data type with duplicated keys.

### Array of Maps

#### RQ.SRS-018.ClickHouse.Map.DataType.ArrayOfMaps
version: 1.0

[ClickHouse] SHALL support `Array(Map(key, value))` data type.

### Nested With Maps

#### RQ.SRS-018.ClickHouse.Map.DataType.NestedWithMaps
version: 1.0

[ClickHouse] SHALL support defining `Map(key, value)` data type inside the [Nested] data type.

### Value Retrieval

#### RQ.SRS-018.ClickHouse.Map.DataType.Value.Retrieval
version: 1.0

[ClickHouse] SHALL support getting the value from a `Map(key, value)` data type using `map[key]` syntax.
If `key` has duplicates then the first `key:value` pair MAY be returned. 

For example,

```sql
SELECT a['key2'] FROM table_map;
```

#### RQ.SRS-018.ClickHouse.Map.DataType.Value.Retrieval.KeyInvalid
version: 1.0

[ClickHouse] SHALL return an error when key does not match the key type.

For example,

```sql
SELECT map(1,2) AS m, m[1024]
```

Exceptions:

* when key is `NULL` the return value MAY be `NULL`
* when key value is not valid for the key type, for example it is out of range for [Integer] type, 
  when reading from a table column it MAY return the default value for key data type

#### RQ.SRS-018.ClickHouse.Map.DataType.Value.Retrieval.KeyNotFound
version: 1.0

[ClickHouse] SHALL return default value for the data type of the value
when there's no corresponding `key` defined in the `Map(key, value)` data type. 


### Converting Tuple(Array, Array) to Map

#### RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.TupleOfArraysToMap
version: 1.0

[ClickHouse] SHALL support converting [Tuple(Array, Array)] to `Map(key, value)` using the [CAST] function.

``` sql
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

``` text
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

#### RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.TupleOfArraysMap.Invalid
version: 1.0

[ClickHouse] MAY return an error when casting [Tuple(Array, Array)] to `Map(key, value)`

* when arrays are not of equal size

  For example,

  ```sql
  SELECT CAST(([2, 1, 1023], ['', '']), 'Map(UInt8, String)') AS map, map[10]
  ```

### Converting Array(Tuple(K,V)) to Map

#### RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.ArrayOfTuplesToMap
version: 1.0

[ClickHouse] SHALL support converting [Array(Tuple(K,V))] to `Map(key, value)` using the [CAST] function.

For example,

```sql
SELECT CAST(([(1,2),(3)]), 'Map(UInt8, UInt8)') AS map
```

#### RQ.SRS-018.ClickHouse.Map.DataType.Conversion.From.ArrayOfTuplesToMap.Invalid
version: 1.0

[ClickHouse] MAY return an error when casting [Array(Tuple(K, V))] to `Map(key, value)`

* when element is not a [Tuple]

  ```sql
  SELECT CAST(([(1,2),(3)]), 'Map(UInt8, UInt8)') AS map
  ```

* when [Tuple] does not contain two elements

  ```sql
  SELECT CAST(([(1,2),(3,)]), 'Map(UInt8, UInt8)') AS map
  ```

### Keys and Values Subcolumns

#### RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Keys
version: 1.0

[ClickHouse] SHALL support `keys` subcolumn in the `Map(key, value)` type that can be used 
to retrieve an [Array] of map keys.

```sql
SELECT m.keys FROM t_map;
```

#### RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Keys.ArrayFunctions
version: 1.0

[ClickHouse] SHALL support applying [Array] functions to the `keys` subcolumn in the `Map(key, value)` type.

For example,

```sql
SELECT * FROM t_map WHERE has(m.keys, 'a');
```

#### RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Keys.InlineDefinedMap
version: 1.0

[ClickHouse] MAY not support using inline defined map to get `keys` subcolumn.

For example,

```sql
SELECT map( 'aa', 4, '44' , 5) as c, c.keys
```

#### RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Values
version: 1.0

[ClickHouse] SHALL support `values` subcolumn in the `Map(key, value)` type that can be used 
to retrieve an [Array] of map values.

```sql
SELECT m.values FROM t_map;
```

#### RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Values.ArrayFunctions
version: 1.0

[ClickHouse] SHALL support applying [Array] functions to the `values` subcolumn in the `Map(key, value)` type.

For example,

```sql
SELECT * FROM t_map WHERE has(m.values, 'a');
```

#### RQ.SRS-018.ClickHouse.Map.DataType.SubColumns.Values.InlineDefinedMap
version: 1.0

[ClickHouse] MAY not support using inline defined map to get `values` subcolumn.

For example,

```sql
SELECT map( 'aa', 4, '44' , 5) as c, c.values
```

### Functions

#### RQ.SRS-018.ClickHouse.Map.DataType.Functions.InlineDefinedMap
version: 1.0

[ClickHouse] SHALL support using inline defined maps as an argument to map functions.

For example,

```sql
SELECT map( 'aa', 4, '44' , 5) as c, mapKeys(c)
SELECT map( 'aa', 4, '44' , 5) as c, mapValues(c)
```

#### `length`

##### RQ.SRS-018.ClickHouse.Map.DataType.Functions.Length
version: 1.0

[ClickHouse] SHALL support `Map(key, value)` data type as an argument to the [length] function
that SHALL return number of keys in the map.

For example,

```sql
SELECT length(map(1,2,3,4))
SELECT length(map())
```

#### `empty`

##### RQ.SRS-018.ClickHouse.Map.DataType.Functions.Empty
version: 1.0

[ClickHouse] SHALL support `Map(key, value)` data type as an argument to the [empty] function
that SHALL return 1 if number of keys in the map is 0 otherwise if the number of keys is 
greater or equal to 1 it SHALL return 0.

For example,

```sql
SELECT empty(map(1,2,3,4))
SELECT empty(map())
```

#### `notEmpty`

##### RQ.SRS-018.ClickHouse.Map.DataType.Functions.NotEmpty
version: 1.0

[ClickHouse] SHALL support `Map(key, value)` data type as an argument to the [notEmpty] function
that SHALL return 0 if number if keys in the map is 0 otherwise if the number of keys is
greater or equal to 1 it SHALL return 1.

For example,

```sql
SELECT notEmpty(map(1,2,3,4))
SELECT notEmpty(map())
```

#### `map`

##### RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map
version: 1.0

[ClickHouse] SHALL support arranging `key, value` pairs into `Map(key, value)` data type
using `map` function.

**Syntax** 

``` sql
map(key1, value1[, key2, value2, ...])
```

For example,

``` sql
SELECT map('key1', number, 'key2', number * 2) FROM numbers(3);

┌─map('key1', number, 'key2', multiply(number, 2))─┐
│ {'key1':0,'key2':0}                              │
│ {'key1':1,'key2':2}                              │
│ {'key1':2,'key2':4}                              │
└──────────────────────────────────────────────────┘
```

##### RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.InvalidNumberOfArguments
version: 1.0

[ClickHouse] SHALL return an error when `map` function is called with non even number of arguments.

##### RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MixedKeyOrValueTypes
version: 1.0

[ClickHouse] SHALL return an error when `map` function is called with mixed key or value types.


##### RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MapAdd
version: 1.0

[ClickHouse] SHALL support converting the results of `mapAdd` function to a `Map(key, value)` data type.

For example,

``` sql
SELECT CAST(mapAdd(([toUInt8(1), 2], [1, 1]), ([toUInt8(1), 2], [1, 1])), "Map(Int8,Int8)")
```

##### RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MapSubstract
version: 1.0

[ClickHouse] SHALL support converting the results of `mapSubstract` function to a `Map(key, value)` data type.

For example,

```sql
SELECT CAST(mapSubtract(([toUInt8(1), 2], [toInt32(1), 1]), ([toUInt8(1), 2], [toInt32(2), 1])), "Map(Int8,Int8)")
```
##### RQ.SRS-018.ClickHouse.Map.DataType.Functions.Map.MapPopulateSeries
version: 1.0

[ClickHouse] SHALL support converting the results of `mapPopulateSeries` function to a `Map(key, value)` data type.

For example,

```sql
SELECT CAST(mapPopulateSeries([1,2,4], [11,22,44], 5), "Map(Int8,Int8)")
```

#### `mapContains`

##### RQ.SRS-018.ClickHouse.Map.DataType.Functions.MapContains
version: 1.0

[ClickHouse] SHALL support `mapContains(map, key)` function to check weather `map.keys` contains the `key`.

For example,

```sql
SELECT mapContains(a, 'abc') from table_map;
```

#### `mapKeys`

##### RQ.SRS-018.ClickHouse.Map.DataType.Functions.MapKeys
version: 1.0

[ClickHouse] SHALL support `mapKeys(map)` function to return all the map keys in the [Array] format.

For example,

```sql
SELECT mapKeys(a) from table_map;
```

#### `mapValues`

##### RQ.SRS-018.ClickHouse.Map.DataType.Functions.MapValues
version: 1.0

[ClickHouse] SHALL support `mapValues(map)` function to return all the map values in the [Array] format.

For example,

```sql
SELECT mapValues(a) from table_map;
```

[Nested]: https://clickhouse.tech/docs/en/sql-reference/data-types/nested-data-structures/nested/
[length]: https://clickhouse.tech/docs/en/sql-reference/functions/array-functions/#array_functions-length
[empty]: https://clickhouse.tech/docs/en/sql-reference/functions/array-functions/#function-empty
[notEmpty]: https://clickhouse.tech/docs/en/sql-reference/functions/array-functions/#function-notempty
[CAST]: https://clickhouse.tech/docs/en/sql-reference/functions/type-conversion-functions/#type_conversion_function-cast
[Tuple]: https://clickhouse.tech/docs/en/sql-reference/data-types/tuple/
[Tuple(Array,Array)]: https://clickhouse.tech/docs/en/sql-reference/data-types/tuple/
[Array]: https://clickhouse.tech/docs/en/sql-reference/data-types/array/ 
[String]: https://clickhouse.tech/docs/en/sql-reference/data-types/string/
[Integer]: https://clickhouse.tech/docs/en/sql-reference/data-types/int-uint/
[ClickHouse]: https://clickhouse.tech
[GitHub Repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/map_type/requirements/requirements.md 
[Revision History]: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/map_type/requirements/requirements.md
[Git]: https://git-scm.com/
[GitHub]: https://github.com
''')
