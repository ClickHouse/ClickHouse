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
