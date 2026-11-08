# Iceberg table from the icebird tests

The table `java/bunnies` is copied from the test data of icebird (an Iceberg reader in JavaScript),
`test/files/hyperparam-iceberg/java/bunnies` at commit
https://github.com/hyparam/icebird/tree/2ae55bcca87a89c3fc93392fe44d0b80afd87435/test/files/hyperparam-iceberg/java/bunnies
(MIT License, see the license text below). No file is modified.

The table was written by Apache Iceberg Java. Its Parquet writer stores columns whose names are not valid Avro
names under sanitized names (`Breed Name` as `Breed_x20Name`), with the Iceberg field ids. The snapshots:
`v2.metadata.json` - 21 rows, `v3.metadata.json` - a column `__happy__` is added, `v4.metadata.json` - six rows
are removed by a position delete file, `v5.metadata.json` (current) - the row `New Zealand` is replaced: an
equality delete file keyed on `Breed Name` and a data file with the updated row.

The location of the table is `s3a://hyperparam-iceberg/java/bunnies`, so the tests run the queries from this
directory with the relative path `java/bunnies`. The expected results are taken from icebird's
`test/read.java.test.js` at the same commit.

## License

```
The MIT License (MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```
