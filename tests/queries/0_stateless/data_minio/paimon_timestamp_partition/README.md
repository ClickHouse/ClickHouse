## Paimon tables partitioned by `TIMESTAMP`

`ts6`, `ts9` and `ltz` are three tables of one Paimon warehouse, generated together by the program
below and kept here in the layout the warehouse has on disk. Everything under `data_minio` is
uploaded to the `test` bucket of minio by `ci/jobs/scripts/functional_tests/setup_minio.sh`, so the
tables are reachable as `paimonS3(s3_conn, filename='paimon_timestamp_partition/<table>')`.

They cover reading a Paimon table whose partition key is a `TIMESTAMP` column, which makes ClickHouse
decode the partition value out of the manifest `BinaryRow` and reproduce the name of the partition
directory:

* `ts6` — `TIMESTAMP(6)`, the precision of a bare Paimon `TIMESTAMP` and of every Spark
  `TIMESTAMP`/`TIMESTAMP_NTZ`, so this is the table from
  https://github.com/ClickHouse/ClickHouse/issues/112768.
* `ts9` — `TIMESTAMP(9)`, plus every branch of the directory naming: no fraction with a zero second,
  no fraction with a non-zero second, and 3, 6 and 9 fractional digits. The last row is before the
  epoch, where the millisecond is negative while `nanoOfMillisecond` still counts forward.
* `ltz` — a `TIMESTAMP(0)` partition key, which uses the compact encoding below precision 3, and a
  `TIMESTAMP WITH LOCAL TIME ZONE` one. Paimon names a partition directory from the raw epoch
  millisecond with no time zone applied, so reading the latter must not depend on the time zone of
  the server. Both values sit far from midnight so that a shifted rendering names a different
  directory.

The directory names Paimon produced, which the reader has to reconstruct exactly:

```
ts9/ts=1960-03-04T05%3A06%3A07.000000891
ts9/ts=2025-07-31T16%3A00
ts9/ts=2025-07-31T16%3A40%3A00.000456
ts9/ts=2025-07-31T16%3A40%3A00.123
ts9/ts=2025-07-31T16%3A40%3A00.123456789
ts9/ts=2025-07-31T16%3A40%3A05
ts6/ts=2025-07-31T16%3A00
ts6/ts=2025-07-31T16%3A40
ts6/ts=2025-07-31T16%3A40%3A00.000456
ts6/ts=2025-07-31T16%3A40%3A00.123
ts6/ts=2025-07-31T16%3A40%3A00.123456
ltz/ts0=2025-07-31T16%3A40%3A05/tsltz=2025-07-31T00%3A30%3A00.123456
ltz/ts0=2025-07-31T17%3A00/tsltz=2025-07-31T23%3A45
```

Note that `tsltz` was written from a machine in a non-UTC time zone and is still named `00%3A30`,
not `08%3A30` — Paimon's `Timestamp.toString` is `toLocalDateTime().toString()` and
`Timestamp.toLocalDateTime` splits the raw epoch millisecond arithmetically.

### Pre-requirements

* Maven: Apache Maven 3.9.9 (8e8579a9e76f7d015ee5ec7bfcdc97d260186937)
* JDK: java 17.0.12 2024-07-16 LTS

### Generate steps

1. Create a Maven project with the `pom.xml` and `DataGenerator.java` below.

2. `pom.xml` — only `paimon-bundle` and Hadoop are needed, the shaded bundle carries the catalog,
   the table and the format writers:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-timestamp-partition-generator</artifactId>
    <version>1.1.1</version>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <paimon.version>1.1.1</paimon.version>
        <hadoop.version>2.8.5</hadoop.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.paimon</groupId>
            <artifactId>paimon-bundle</artifactId>
            <version>${paimon.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>${hadoop.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs-client</artifactId>
            <version>${hadoop.version}</version>
        </dependency>
    </dependencies>
</project>
```

3. `src/main/java/org/apache/paimon/service/example/DataGenerator.java`:

```java
package org.apache.paimon.service.example;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

public class DataGenerator
{
    private static Catalog getCatalog(String rootPath)
    {
        return CatalogFactory.createCatalog(CatalogContext.create(new Path(rootPath)));
    }

    private static void createAndWrite(
            String rootPath, String table, Schema schema, List<InternalRow> rows) throws Exception
    {
        Identifier identifier = Identifier.create("tests", table);
        Catalog catalog = getCatalog(rootPath);
        catalog.createDatabase("tests", true);
        catalog.createTable(identifier, schema, false);

        Table paimonTable = catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = paimonTable.newBatchWriteBuilder();
        TableWriteImpl<?> writer =
                (TableWriteImpl<?>) writeBuilder.newWrite().withIOManager(new IOManagerImpl(rootPath));
        for (InternalRow row : rows)
            writer.write(row);
        List<CommitMessage> messages = writer.prepareCommit();
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);
        writer.close();
    }

    private static Timestamp ts(String text)
    {
        return Timestamp.fromLocalDateTime(LocalDateTime.parse(text));
    }

    private static void generateTs9(String rootPath) throws Exception
    {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("ts", DataTypes.TIMESTAMP(9).notNull())
                .partitionKeys("ts")
                .build();

        List<InternalRow> rows = Arrays.asList(
                GenericRow.of(1, ts("2025-07-31T16:00:00")),
                GenericRow.of(2, ts("2025-07-31T16:40:05")),
                GenericRow.of(3, ts("2025-07-31T16:40:00.123")),
                GenericRow.of(4, ts("2025-07-31T16:40:00.000456")),
                GenericRow.of(5, ts("2025-07-31T16:40:00.123456789")),
                GenericRow.of(6, ts("1960-03-04T05:06:07.000000891")));

        createAndWrite(rootPath, "ts9", schema, rows);
    }

    private static void generateTs6(String rootPath) throws Exception
    {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("ts", DataTypes.TIMESTAMP(6).notNull())
                .partitionKeys("ts")
                .build();

        List<InternalRow> rows = Arrays.asList(
                GenericRow.of(1, ts("2025-07-31T16:40:00.123456")),
                GenericRow.of(2, ts("2025-07-31T16:40:00.000456")),
                GenericRow.of(3, ts("2025-07-31T16:40:00.123")),
                GenericRow.of(4, ts("2025-07-31T16:40:00")),
                GenericRow.of(5, ts("2025-07-31T16:00:00")));

        createAndWrite(rootPath, "ts6", schema, rows);
    }

    private static void generateLtz(String rootPath) throws Exception
    {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("ts0", DataTypes.TIMESTAMP(0).notNull())
                .column("tsltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6).notNull())
                .partitionKeys("ts0", "tsltz")
                .build();

        List<InternalRow> rows = Arrays.asList(
                GenericRow.of(1, ts("2025-07-31T16:40:05"), ts("2025-07-31T00:30:00.123456")),
                GenericRow.of(2, ts("2025-07-31T17:00:00"), ts("2025-07-31T23:45:00")));

        createAndWrite(rootPath, "ltz", schema, rows);
    }

    public static void main(String[] args) throws Exception
    {
        String rootPath = args[0];
        generateTs9(rootPath);
        generateTs6(rootPath);
        generateLtz(rootPath);
        System.out.println("generated into " + rootPath);
    }
}
```

4. Build and run, then copy `<warehouse>/tests.db/{ts6,ts9,ltz}` next to this file:

```
mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt
mvn -q compile
java -cp "target/classes:$(cat cp.txt)" org.apache.paimon.service.example.DataGenerator /tmp/warehouse
```
