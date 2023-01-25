# SRS020 ClickHouse Extended Precision Data Types
# Software Requirements Specification

## Table of Contents

* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
* 3 [Terminology](#terminology)
  * 3.1 [Extended Precision Data Types](#extended-precision-data-types)
* 4 [Requirements](#requirements)
  * 4.1 [RQ.SRS-020.ClickHouse.Extended.Precision](#rqsrs-020clickhouseextendedprecision)
  * 4.2 [Conversion](#conversion)
    * 4.2.1 [RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.toInt128](#rqsrs-020clickhouseextendedprecisionconversiontoint128)
    * 4.2.2 [RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.toUInt128](#rqsrs-020clickhouseextendedprecisionconversiontouint128)
    * 4.2.3 [RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.toInt256](#rqsrs-020clickhouseextendedprecisionconversiontoint256)
    * 4.2.4 [RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.toUInt256](#rqsrs-020clickhouseextendedprecisionconversiontouint256)
    * 4.2.5 [RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.toDecimal256](#rqsrs-020clickhouseextendedprecisionconversiontodecimal256)
    * 4.2.6 [RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.FromMySQL](#rqsrs-020clickhouseextendedprecisionconversionfrommysql)
    * 4.2.7 [RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.ToMySQL](#rqsrs-020clickhouseextendedprecisionconversiontomysql)
  * 4.3 [Arithmetic](#arithmetic)
    * 4.3.1 [RQ.SRS-020.ClickHouse.Extended.Precision.Arithmetic.Int.Supported](#rqsrs-020clickhouseextendedprecisionarithmeticintsupported)
    * 4.3.2 [RQ.SRS-020.ClickHouse.Extended.Precision.Arithmetic.Dec.Supported](#rqsrs-020clickhouseextendedprecisionarithmeticdecsupported)
    * 4.3.3 [RQ.SRS-020.ClickHouse.Extended.Precision.Arithmetic.Dec.NotSupported](#rqsrs-020clickhouseextendedprecisionarithmeticdecnotsupported)
  * 4.4 [Arrays](#arrays)
    * 4.4.1 [RQ.SRS-020.ClickHouse.Extended.Precision.Arrays.Int.Supported](#rqsrs-020clickhouseextendedprecisionarraysintsupported)
    * 4.4.2 [RQ.SRS-020.ClickHouse.Extended.Precision.Arrays.Int.NotSupported](#rqsrs-020clickhouseextendedprecisionarraysintnotsupported)
    * 4.4.3 [RQ.SRS-020.ClickHouse.Extended.Precision.Arrays.Dec.Supported](#rqsrs-020clickhouseextendedprecisionarraysdecsupported)
    * 4.4.4 [RQ.SRS-020.ClickHouse.Extended.Precision.Arrays.Dec.NotSupported](#rqsrs-020clickhouseextendedprecisionarraysdecnotsupported)
  * 4.5 [Comparison](#comparison)
    * 4.5.1 [RQ.SRS-020.ClickHouse.Extended.Precision.Comparison](#rqsrs-020clickhouseextendedprecisioncomparison)
  * 4.6 [Logical Functions](#logical-functions)
    * 4.6.1 [RQ.SRS-020.ClickHouse.Extended.Precision.Logical](#rqsrs-020clickhouseextendedprecisionlogical)
  * 4.7 [Mathematical Functions](#mathematical-functions)
    * 4.7.1 [RQ.SRS-020.ClickHouse.Extended.Precision.Mathematical.Supported](#rqsrs-020clickhouseextendedprecisionmathematicalsupported)
    * 4.7.2 [RQ.SRS-020.ClickHouse.Extended.Precision.Mathematical.NotSupported](#rqsrs-020clickhouseextendedprecisionmathematicalnotsupported)
  * 4.8 [Rounding Functions](#rounding-functions)
    * 4.8.1 [RQ.SRS-020.ClickHouse.Extended.Precision.Rounding.Int.Supported](#rqsrs-020clickhouseextendedprecisionroundingintsupported)
    * 4.8.2 [RQ.SRS-020.ClickHouse.Extended.Precision.Rounding.Int.NotSupported](#rqsrs-020clickhouseextendedprecisionroundingintnotsupported)
    * 4.8.3 [RQ.SRS-020.ClickHouse.Extended.Precision.Rounding.Dec.Supported](#rqsrs-020clickhouseextendedprecisionroundingdecsupported)
    * 4.8.4 [RQ.SRS-020.ClickHouse.Extended.Precision.Rounding.Dec.NotSupported](#rqsrs-020clickhouseextendedprecisionroundingdecnotsupported)
  * 4.9 [Bit Functions](#bit-functions)
    * 4.9.1 [RQ.SRS-020.ClickHouse.Extended.Precision.Bit.Int.Supported](#rqsrs-020clickhouseextendedprecisionbitintsupported)
    * 4.9.2 [RQ.SRS-020.ClickHouse.Extended.Precision.Bit.Int.NotSupported](#rqsrs-020clickhouseextendedprecisionbitintnotsupported)
    * 4.9.3 [RQ.SRS-020.ClickHouse.Extended.Precision.Bit.Dec.NotSupported](#rqsrs-020clickhouseextendedprecisionbitdecnotsupported)
  * 4.10 [Null Functions](#null-functions)
    * 4.10.1 [RQ.SRS-020.ClickHouse.Extended.Precision.Null](#rqsrs-020clickhouseextendedprecisionnull)
  * 4.11 [Tuple Functions](#tuple-functions)
    * 4.11.1 [RQ.SRS-020.ClickHouse.Extended.Precision.Tuple](#rqsrs-020clickhouseextendedprecisiontuple)
  * 4.12 [Map Functions](#map-functions)
    * 4.12.1 [RQ.SRS-020.ClickHouse.Extended.Precision.Map.Supported](#rqsrs-020clickhouseextendedprecisionmapsupported)
    * 4.12.2 [RQ.SRS-020.ClickHouse.Extended.Precision.Map.NotSupported](#rqsrs-020clickhouseextendedprecisionmapnotsupported)
  * 4.13 [Create](#create)
    * 4.13.1 [RQ.SRS-020.ClickHouse.Extended.Precision.Create.Table](#rqsrs-020clickhouseextendedprecisioncreatetable)
* 5 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software
hosted in a [GitHub Repository].
All the updates are tracked using the [Revision History].

## Introduction

This software requirements specification covers requirements related to [ClickHouse]
using extended precision data types.

## Terminology

### Extended Precision Data Types

Inclusive bounds:
* Int128 - [-170141183460469231731687303715884105728 : 170141183460469231731687303715884105727]
* UInt128 - [0 : 340282366920938463463374607431768211455]
* Int256 - [-57896044618658097711785492504343953926634992332820282019728792003956564819968 : 57896044618658097711785492504343953926634992332820282019728792003956564819967]
* UInt256 - [0 : 115792089237316195423570985008687907853269984665640564039457584007913129639935]

Exclusive bounds:
* Decimal256 - (10^(76 - S): 10^(76 - S)), where S is the scale.

## Requirements

### RQ.SRS-020.ClickHouse.Extended.Precision
version: 1.0

[ClickHouse] SHALL support using [Extended Precision Data Types].

### Conversion

#### RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.toInt128
version: 1.0

[ClickHouse] SHALL support converting values to `Int128` using the `toInt128` function.

For example,

```sql
SELECT toInt128(1)
```

#### RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.toUInt128
version: 1.0

[ClickHouse] SHALL support converting values to `UInt128` format using `toUInt128` function.

For example,

```sql
SELECT toUInt128(1)
```

#### RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.toInt256
version: 1.0

[ClickHouse] SHALL support converting values to `Int256` using `toInt256` function.

For example,

```sql
SELECT toInt256(1)
```

#### RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.toUInt256
version: 1.0

[ClickHouse] SHALL support converting values to `UInt256` format using `toUInt256` function.

For example,

```sql
SELECT toUInt256(1)
```

#### RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.toDecimal256
version: 1.0

[ClickHouse] SHALL support converting values to `Decimal256` format using `toDecimal256` function.

For example,

```sql
SELECT toDecimal256(1,2)
```

#### RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.FromMySQL
version: 1.0

[ClickHouse] SHALL support converting to [Extended Precision Data Types] from MySQL.


#### RQ.SRS-020.ClickHouse.Extended.Precision.Conversion.ToMySQL
version: 1.0

[ClickHouse] MAY not support converting from [Extended Precision Data Types] to MySQL.

### Arithmetic

#### RQ.SRS-020.ClickHouse.Extended.Precision.Arithmetic.Int.Supported
version: 1.0

[ClickHouse] SHALL support using [Arithmetic functions] with Int128, UInt128, Int256, and UInt256.

Arithmetic functions:
* plus
* minus
* multiply
* divide
* intDiv
* intDivOrZero
* modulo
* moduloOrZero
* negate
* abs
* gcd
* lcm

#### RQ.SRS-020.ClickHouse.Extended.Precision.Arithmetic.Dec.Supported
version: 1.0

[ClickHouse] SHALL support using the following [Arithmetic functions] with Decimal256:

* plus
* minus
* multiply
* divide
* intDiv
* intDivOrZero
* negate
* abs

#### RQ.SRS-020.ClickHouse.Extended.Precision.Arithmetic.Dec.NotSupported
version: 1.0

[ClickHouse] MAY not support using the following [Arithmetic functions] with Decimal256:

* modulo
* moduloOrZero
* gcd
* lcm

### Arrays

#### RQ.SRS-020.ClickHouse.Extended.Precision.Arrays.Int.Supported
version: 1.0

[ClickHouse] SHALL support using the following [Array functions] with Int128, UInt128, Int256, and UInt256.

* empty
* notEmpty
* length
* arrayCount
* arrayPopBack
* arrayPopFront
* arraySort
* arrayReverseSort
* arrayUniq
* arrayJoin
* arrayDistinct
* arrayEnumerate
* arrayEnumerateDense
* arrayEnumerateUniq
* arrayReverse
* reverse
* arrayFlatten
* arrayCompact
* arrayExists
* arrayAll
* arrayMin
* arrayMax
* arraySum
* arrayAvg
* arrayReduce
* arrayReduceInRanges
* arrayZip
* arrayMap
* arrayFilter
* arrayFill
* arrayReverseFill
* arraySplit
* arrayFirst
* arrayFirstIndex
* arrayConcat
* hasAll
* hasAny
* hasSubstr
* arrayElement
* has
* indexOf
* countEqual
* arrayPushBack
* arrayPushFront
* arrayResize
* arraySlice

#### RQ.SRS-020.ClickHouse.Extended.Precision.Arrays.Int.NotSupported
version: 1.0

[ClickHouse] MAY not support using the following [Array functions] with Int128, UInt128, Int256, and UInt256:

* arrayDifference
* arrayCumSum
* arrayCumSumNonNegative

#### RQ.SRS-020.ClickHouse.Extended.Precision.Arrays.Dec.Supported
version: 1.0

[ClickHouse] SHALL support using the following [Array functions] with Decimal256:

* empty
* notEmpty
* length
* arrayCount
* arrayPopBack
* arrayPopFront
* arraySort
* arrayReverseSort
* arrayUniq
* arrayJoin
* arrayDistinct
* arrayEnumerate
* arrayEnumerateDense
* arrayEnumerateUniq
* arrayReverse
* reverse
* arrayFlatten
* arrayCompact
* arrayExists
* arrayAll
* arrayReduce
* arrayReduceInRanges
* arrayZip
* arrayMap
* arrayFilter
* arrayFill
* arrayReverseFill
* arraySplit
* arrayFirst
* arrayFirstIndex
* arrayConcat
* hasAll
* hasAny
* hasSubstr
* arrayElement
* has
* indexOf
* countEqual
* arrayPushBack
* arrayPushFront
* arrayResize
* arraySlice

#### RQ.SRS-020.ClickHouse.Extended.Precision.Arrays.Dec.NotSupported
version: 1.0

[ClickHouse] MAY not support using the following [Array functions] with Decimal256:

* arrayMin
* arrayMax
* arraaySum
* arrayAvg
* arrayDifference
* arrayCumSum
* arrayCumSumNonNegative

### Comparison

#### RQ.SRS-020.ClickHouse.Extended.Precision.Comparison
version: 1.0

[ClickHouse] SHALL support using [Comparison functions] with [Extended Precision Data Types].

Comparison functions:
* equals
* notEquals
* less
* greater
* lessOrEquals
* greaterOrEquals

### Logical Functions

#### RQ.SRS-020.ClickHouse.Extended.Precision.Logical
version: 1.0

[ClickHouse] MAY not support using [Logical functions] with [Extended Precision Data Types].

Logical functions:
* and
* or
* not
* xor

### Mathematical Functions

#### RQ.SRS-020.ClickHouse.Extended.Precision.Mathematical.Supported
version: 1.0

[ClickHouse] SHALL support using the following [Mathematical functions] with [Extended Precision Data Types]:

* exp
* log, ln
* exp2
* log2
* exp10
* log10
* sqrt
* cbrt
* erf
* erfc
* lgamma
* tgamma
* sin
* cos
* tan
* asin
* acos
* atan
* cosh
* acosh
* sinh
* asinh
* tanh
* atanh
* log1p
* sign

#### RQ.SRS-020.ClickHouse.Extended.Precision.Mathematical.NotSupported
version: 1.0

[ClickHouse] MAY not support using the following [Mathematical functions] with [Extended Precision Data Types]:

* pow, power
* intExp2
* intExp10
* atan2
* hypot

### Rounding Functions

#### RQ.SRS-020.ClickHouse.Extended.Precision.Rounding.Int.Supported
version: 1.0

[ClickHouse] SHALL support using the following [Rounding functions] with Int128, UInt128, Int256, and UInt256:

* floor
* ceil
* trunc
* round
* roundBankers
* roundDuration
* roundAge

#### RQ.SRS-020.ClickHouse.Extended.Precision.Rounding.Int.NotSupported
version: 1.0

[ClickHouse] MAY not support using the following [Rounding functions] with Int128, UInt128, Int256, and UInt256:

* roundDown
* roundToExp2

#### RQ.SRS-020.ClickHouse.Extended.Precision.Rounding.Dec.Supported
version: 1.0

[ClickHouse] SHALL support using the following [Rounding functions] with Decimal256:

* floor
* ceil
* trunc
* round
* roundBankers

#### RQ.SRS-020.ClickHouse.Extended.Precision.Rounding.Dec.NotSupported
version: 1.0

[ClickHouse] MAY not support using the following [Rounding functions] with Decimal256:

* roundDuration
* roundAge
* roundDown
* roundToExp2

### Bit Functions

#### RQ.SRS-020.ClickHouse.Extended.Precision.Bit.Int.Supported
version: 1.0

[ClickHouse] SHALL support using the following [Bit functions] with Int128, UInt128, Int256, and UInt256:

* bitAnd
* bitOr
* bitXor
* bitNot
* bitShiftLeft
* bitShiftRight
* bitCount

#### RQ.SRS-020.ClickHouse.Extended.Precision.Bit.Int.NotSupported
version: 1.0

[ClickHouse] MAY not support using the following [Bit functions] with Int128, UInt128, Int256, and UInt256:

* bitRotateLeft
* bitRotateRight
* bitTest
* bitTestAll
* bitTestAny

#### RQ.SRS-020.ClickHouse.Extended.Precision.Bit.Dec.NotSupported
version: 1.0

[ClickHouse] MAY not support using [Bit functions] with Decimal256.

Bit functions:
* bitAnd
* bitOr
* bitXor
* bitNot
* bitShiftLeft
* bitShiftRight
* bitCount
* bitRotateLeft
* bitRotateRight
* bitTest
* bitTestAll
* bitTestAny

### Null Functions

#### RQ.SRS-020.ClickHouse.Extended.Precision.Null
version: 1.0

[ClickHouse] SHALL support using [Null functions] with [Extended Precision Data Types].

Null functions:
* isNull
* isNotNull
* coalesce
* ifNull
* nullIf
* assumeNotNull
* toNullable

### Tuple Functions

#### RQ.SRS-020.ClickHouse.Extended.Precision.Tuple
version: 1.0

[ClickHouse] SHALL support using [Tuple functions] with [Extended Precision Data Types].

Tuple functions:
* tuple
* tupleElement
* untuple

### Map Functions

#### RQ.SRS-020.ClickHouse.Extended.Precision.Map.Supported
version: 1.0

[ClickHouse] SHALL support using the following [Map functions] with [Extended Precision Data Types]:

* map
* mapContains
* mapKeys
* mapValues

#### RQ.SRS-020.ClickHouse.Extended.Precision.Map.NotSupported
version: 1.0

[ClickHouse] MAY not support using the following [Map functions] with [Extended Precision Data Types]:

* mapAdd
* mapSubtract
* mapPopulateSeries

### Create

#### RQ.SRS-020.ClickHouse.Extended.Precision.Create.Table
version: 1.0

[ClickHouse] SHALL support creating table with columns that use [Extended Precision Data Types].

## References

* **ClickHouse:** https://clickhouse.com
* **GitHub Repository**: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/extended_precision_data_types/requirements/requirements.md
* **Revision History**: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/extended_precision_data_types/requirements/requirements.md
* **Git:** https://git-scm.com/

[Extended Precision Data Types]: #extended-precision-data-types
[Arithmetic functions]: https://clickhouse.com/docs/en/sql-reference/functions/arithmetic-functions/
[Array functions]: https://clickhouse.com/docs/en/sql-reference/functions/array-functions/
[Comparison functions]: https://clickhouse.com/docs/en/sql-reference/functions/comparison-functions/
[Logical Functions]: https://clickhouse.com/docs/en/sql-reference/functions/logical-functions/
[Mathematical Functions]: https://clickhouse.com/docs/en/sql-reference/functions/math-functions/
[Rounding Functions]: https://clickhouse.com/docs/en/sql-reference/functions/rounding-functions/
[Bit Functions]: https://clickhouse.com/docs/en/sql-reference/functions/bit-functions/
[Null Functions]: https://clickhouse.com/docs/en/sql-reference/functions/functions-for-nulls/
[Tuple Functions]: https://clickhouse.com/docs/en/sql-reference/functions/tuple-functions/
[Map Functions]: https://clickhouse.com/docs/en/sql-reference/functions/tuple-map-functions/
[SRS]: #srs
[ClickHouse]: https://clickhouse.com
[GitHub Repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/extended_precision_data_types/requirements/requirements.md
[Revision History]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/extended_precision_data_types/requirements/requirements.md
[Git]: https://git-scm.com/
[GitHub]: https://github.com
