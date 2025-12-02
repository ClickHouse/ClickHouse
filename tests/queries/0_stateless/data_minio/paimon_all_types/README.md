## How to Generate a Paimon Format Directory
This directory is generated using the Paimon Java client.

### Pre-Requirements
* Maven: Apache Maven 3.9.9 (8e8579a9e76f7d015ee5ec7bfcdc97d260186937)
* JDK: java 17.0.12 2024-07-16 LTS


### Generate steps
1. Create a Maven project

2. Create pom.xml
Replace mainClass with your actual class name in the configuration below
```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <version>1.1.1</version>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-example</artifactId>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <hadoop.version>2.8.5</hadoop.version>
        <log4j.version>2.17.1</log4j.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.apache.paimon</groupId>
            <artifactId>paimon-common</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.paimon</groupId>
            <artifactId>paimon-core</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>${hadoop.version}</version>
            <scope>runtime</scope>
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
            <scope>runtime</scope>
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
            <groupId>org.apache.paimon</groupId>
            <artifactId>paimon-format</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>1.20.1</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>1.20.1</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.paimon</groupId>
            <artifactId>paimon-flink-common</artifactId>
            <version>1.1-SNAPSHOT</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-compress</artifactId>
            <version>1.24.0</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.flink/flink-connector-base -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-base</artifactId>
            <version>1.20.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-runtime</artifactId>
            <version>1.20.1</version>
            <scope>compile</scope> <!-- 或 runtime，取决于部署方式 -->
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-java-bridge</artifactId>
            <version>1.20.1</version>
            <scope>compile</scope>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.flink/flink-table-planner -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner_2.12</artifactId>
            <version>1.20.1</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>1.20.1</version> <!-- 与你的 Flink 版本完全一致 -->
            <scope>runtime</scope> <!-- 运行时必须存在 -->
        </dependency>

        <dependency>
            <groupId>commons-io</groupId>
            <artifactId>commons-io</artifactId>
            <version>2.11.0</version> <!-- 使用最新稳定版本 -->
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>1.20.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>1.20.1</version>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>3.0.0</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>java</goal>
                        </goals>
                    </execution>
                </executions>
                <configuration>
                    <addResourcesToClasspath>true</addResourcesToClasspath>
                    <mainClass>org.apache.paimon.service.example.DataGenerator</mainClass>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

3. Create a Data Generation Class
Replace rootPath with your target directory path in the code below
```
package org.apache.paimon.service.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.*;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.*;
import org.apache.paimon.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

    public static Catalog createFilesystemCatalog(String path) {
        CatalogContext context = CatalogContext.create(new Path(path));
        return CatalogFactory.createCatalog(context);
    }

    private static Catalog getCatalog(String rootPath) {
        try {
            return org.apache.paimon.catalog.CatalogFactory.createCatalog(CatalogContext.create(new Path(rootPath)));
        } catch (Exception e) {
            throw new RuntimeException("Create Catalog failed", e);
        }
    }


    private static InternalRow createRow(int id, RowType rowType) {
        GenericRow row = new GenericRow(rowType.getFieldCount());

        List<DataType> fieldTypes = rowType.getFieldTypes();
        List<String> fieldNames = rowType.getFieldNames();

        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            DataType fieldType = fieldTypes.get(i);

            switch (fieldName) {
                case "f_boolean":
                case "f_boolean_nn":
                    row.setField(i, id % 2 == 0);
                    break;
                case "f_char":
                case "f_char_nn":
                    row.setField(i, BinaryString.fromString(String.valueOf((char)('A' + id%26))));
                    break;
                case "f_varchar":
                case "f_varchar_nn":
                    row.setField(i, BinaryString.fromString(String.valueOf((char)('a' + id%26))));
                    break;
                case "f_string":
                case "f_string_nn":
                    row.setField(i, BinaryString.fromString("中文String" + id));
                    break;
                case "f_binary":
                case "f_varbinary":
                case "f_bytes":
                case "f_binary_nn":
                case "f_varbinary_nn":
                case "f_bytes_nn":
                    row.setField(i, new byte[]{(byte)id});
                    break;
                case "f_decimal":
                case "f_decimal_nn":
                    row.setField(i, Decimal.fromBigDecimal(
                            new BigDecimal(id).setScale(1),
                            ((DecimalType) fieldType).getPrecision(),
                            ((DecimalType) fieldType).getScale()
                    ));
                    break;
                case "f_decimal2":
                case "f_decimal2_nn":
                    row.setField(i, Decimal.fromBigDecimal(
                            new BigDecimal(id * 10L).setScale(1),
                            ((DecimalType) fieldType).getPrecision(),
                            ((DecimalType) fieldType).getScale()
                    ));
                    break;
                case "f_decimal3":
                case "f_decimal3_nn":
                    row.setField(i, Decimal.fromBigDecimal(
                            new BigDecimal(id * 100L).setScale(1),
                            ((DecimalType) fieldType).getPrecision(),
                            ((DecimalType) fieldType).getScale()
                    ));
                    break;
                case "f_tinyint":
                case "f_tinyint_nn":
                    row.setField(i, (byte)id);
                    break;
                case "f_smallint":
                case "f_smallint_nn":
                    row.setField(i, (short)id);
                    break;
                case "f_int":
                case "f_int_nn":
                    row.setField(i, id);
                    break;
                case "f_bigint":
                case "f_bigint_nn":
                    row.setField(i, (long)id * 1000);
                    break;
                case "f_float":
                case "f_float_nn":
                    row.setField(i, (float)id + 0.1f);
                    break;
                case "f_double":
                case "f_double_nn":
                    row.setField(i, (double)id + 0.01);
                    break;
                case "f_date":
                case "f_date_nn":
                    row.setField(i, (int)(LocalDate.of(2023, 1, Math.max(1, id % 31)).toEpochDay()));
                    break;
                case "f_time":
                case "f_time_nn":
                    LocalTime time = LocalTime.of(id % 24, id % 60, id % 60);
                    row.setField(i, time.toSecondOfDay() * 1000);
                    break;
                case "f_timestamp":
                case "f_timestamp2":
                case "f_timestamp3":
                case "f_timestamp_nn":
                case "f_timestamp2_nn":
                case "f_timestamp3_nn":
                    LocalDateTime timestamp = LocalDateTime.of(2025, 1, id % 31 + 1, id % 24, id % 60, id % 60, id * 1000 * 1000);
                    row.setField(i, Timestamp.fromLocalDateTime(timestamp));
                    break;
                case "f_array":
                    GenericArray arrayData = new GenericArray(new int[]{id, id*2, id*3});
                    row.setField(i, arrayData);
                    break;
                case "f_map":
                    Map<BinaryString, BinaryString> data = new HashMap<>();
                    data.put(BinaryString.fromString(Integer.toString(id)), BinaryString.fromString(Integer.toString(id)));
                    data.put(BinaryString.fromString(Integer.toString(id*2)), BinaryString.fromString(Integer.toString(id*2)));
                    data.put(BinaryString.fromString(Integer.toString(id*3)), BinaryString.fromString(Integer.toString(id*3)));
                    GenericMap mapData = new GenericMap(data);
                    row.setField(i, mapData);
                    break;
                default:
                    throw new RuntimeException("unknown column name: "+fieldName);
            }
            if ((!fieldName.endsWith("_nn") && !fieldType.is(DataTypeRoot.ARRAY) && !fieldType.is(DataTypeRoot.MAP)) && id % 2 == 0) {
                row.setField(i, null);
            }
        }

        return row;
    }

    public static void generateTestCase1(String rootPath) throws Exception {
        {
            ///  create table
            Schema.Builder schemaBuilder = Schema.newBuilder();
            schemaBuilder.column("f_boolean", DataTypes.BOOLEAN());
            schemaBuilder.column("f_char", DataTypes.CHAR(1));
            schemaBuilder.column("f_varchar", DataTypes.VARCHAR(1));
            schemaBuilder.column("f_string", DataTypes.STRING());
            schemaBuilder.column("f_binary", DataTypes.BINARY(1));
            schemaBuilder.column("f_varbinary", DataTypes.VARBINARY(1));
            schemaBuilder.column("f_bytes", DataTypes.BYTES());
            schemaBuilder.column("f_decimal", DataTypes.DECIMAL(9, 1));
            schemaBuilder.column("f_decimal2", DataTypes.DECIMAL(18, 1));
            schemaBuilder.column("f_decimal3", DataTypes.DECIMAL(38, 1));
            schemaBuilder.column("f_tinyint", DataTypes.TINYINT());
            schemaBuilder.column("f_smallint", DataTypes.SMALLINT());
            schemaBuilder.column("f_int", DataTypes.INT());
            schemaBuilder.column("f_bigint", DataTypes.BIGINT());
            schemaBuilder.column("f_float", DataTypes.FLOAT());
            schemaBuilder.column("f_double", DataTypes.DOUBLE());
            schemaBuilder.column("f_date", DataTypes.DATE());
            schemaBuilder.column("f_time", DataTypes.TIME());
            schemaBuilder.column("f_timestamp", DataTypes.TIMESTAMP(3));
            schemaBuilder.column("f_timestamp2", DataTypes.TIMESTAMP(1));
            schemaBuilder.column("f_timestamp3", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
            schemaBuilder.column("f_boolean_nn", DataTypes.BOOLEAN().notNull());
            schemaBuilder.column("f_char_nn", DataTypes.CHAR(1).notNull());
            schemaBuilder.column("f_varchar_nn", DataTypes.VARCHAR(1).notNull());
            schemaBuilder.column("f_string_nn", DataTypes.STRING().notNull());
            schemaBuilder.column("f_binary_nn", DataTypes.BINARY(1).notNull());
            schemaBuilder.column("f_varbinary_nn", DataTypes.VARBINARY(1).notNull());
            schemaBuilder.column("f_bytes_nn", DataTypes.BYTES().notNull());
            schemaBuilder.column("f_decimal_nn", DataTypes.DECIMAL(9, 1).notNull());
            schemaBuilder.column("f_decimal2_nn", DataTypes.DECIMAL(18, 1).notNull());
            schemaBuilder.column("f_decimal3_nn", DataTypes.DECIMAL(38, 1).notNull());
            schemaBuilder.column("f_tinyint_nn", DataTypes.TINYINT().notNull());
            schemaBuilder.column("f_smallint_nn", DataTypes.SMALLINT().notNull());
            schemaBuilder.column("f_int_nn", DataTypes.INT().notNull());
            schemaBuilder.column("f_bigint_nn", DataTypes.BIGINT().notNull());
            schemaBuilder.column("f_float_nn", DataTypes.FLOAT().notNull());
            schemaBuilder.column("f_double_nn", DataTypes.DOUBLE().notNull());
            schemaBuilder.column("f_date_nn", DataTypes.DATE().notNull());
            schemaBuilder.column("f_time_nn", DataTypes.TIME().notNull());
            schemaBuilder.column("f_timestamp_nn", DataTypes.TIMESTAMP(3).notNull());
            schemaBuilder.column("f_timestamp2_nn", DataTypes.TIMESTAMP(1).notNull());
            schemaBuilder.column("f_timestamp3_nn", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull());
            schemaBuilder.column("f_array", DataTypes.ARRAY(DataTypes.INT().notNull()).notNull());
            schemaBuilder.column("f_map", DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.STRING().notNull()).notNull());

            schemaBuilder.partitionKeys(
                    "f_boolean",
                    "f_string",
                    "f_date",
                    "f_boolean_nn",
                    "f_char_nn",
                    "f_varchar_nn",
                    "f_string_nn",
                    "f_decimal_nn",
                    "f_decimal2_nn",
                    "f_decimal3_nn",
                    "f_tinyint_nn",
                    "f_smallint_nn",
                    "f_int_nn",
                    "f_bigint_nn",
                    "f_float_nn",
                    "f_double_nn",
                    "f_date_nn",
                    "f_time_nn",
                    "f_timestamp_nn",
                    "f_timestamp2_nn");
            Schema schema = schemaBuilder.build();

            Identifier identifier = Identifier.create("tests", "cases2");
            try {
                Catalog catalog = createFilesystemCatalog(rootPath);
                catalog.createDatabase("tests", true);
                catalog.createTable(identifier, schema, false);
            } catch (Catalog.TableAlreadyExistException e) {
                // do something
            } catch (Catalog.DatabaseNotExistException e) {
                // do something
            } catch (Catalog.DatabaseAlreadyExistException e) {
                throw new RuntimeException(e);
            }

            Catalog catalog = getCatalog(rootPath);
            Identifier tableId = Identifier.create("tests", "cases2");

            Table table = catalog.getTable(tableId);
            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            TableWriteImpl writer = (TableWriteImpl) writeBuilder.newWrite()
                    .withIOManager(new IOManagerImpl(rootPath));
            for (int i = 0; i < 10; i++) {
                InternalRow row = createRow(i, table.rowType());
                writer.write(row);
            }
            List<CommitMessage> messages = writer.prepareCommit();
            BatchTableCommit commit = writeBuilder.newCommit();
            commit.commit(messages);
        }
    }

    public static void main(String[] args) throws Exception {
        generateTestCase1("/tmp/warehouse");
    }
}
```

4. Build the project
```
mvn install -DskipTests -Dcheckstyle.skip=true  -Dspotless.check.skip=true -Drat.skip=true -Denforcer.skip
```

5. Run
```
mvn exec:java -Dexec.mainClass=org.apache.paimon.service.example.Example -Dcheckstyle.skip=true
```

Final project tree
```
➜  paimon-example git:(main) ✗ tree -I "target" . 
.
├── pom.xml
└── src
    └── main
        └── java
            └── org
                └── apache
                    └── paimon
                        └── service
                            └── example
                                └── DataGenerator.java

9 directories, 2 files
```