## How to Generate This Paimon Primary-Key Directory

This directory holds an Apache Paimon **primary-key (LSM) table** with two snapshots, generated
using the Paimon Java client. Snapshot 1 inserts `(1, 'old'), (2, 'two')`; snapshot 2 upserts
`(1, 'new')`, which supersedes `(1, 'old')`. Both row versions of `id = 1` therefore live in two
distinct data files, which is what makes this fixture usable for testing merge-on-read behaviour:
the correct answer is `1 new / 2 two`, while the raw union of the data files is
`1 new / 1 old / 2 two`.

### Pre-Requirements
* Maven: Apache Maven 3.8.7
* JDK: java 21 (`maven.compiler.source`/`target` are pinned to 17)

### Generate steps
1. Create a Maven project

2. Create `pom.xml`
```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-pk-example</artifactId>
    <version>1.1.1</version>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.paimon</groupId>
            <artifactId>paimon-bundle</artifactId>
            <version>1.1.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.8.5</version>
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
            <version>2.8.5</version>
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
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>1.7.36</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>3.0.0</version>
                <configuration>
                    <mainClass>gen.PkDataGenerator</mainClass>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

3. Create `src/main/java/gen/PkDataGenerator.java`
```
package gen;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;

import java.util.Collections;
import java.util.List;

public class PkDataGenerator {

    private static Catalog createFilesystemCatalog(String path) {
        CatalogContext context = CatalogContext.create(new Path(path));
        return CatalogFactory.createCatalog(context);
    }

    private static void commit(Table table, List<InternalRow> rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite writer = writeBuilder.newWrite()) {
            for (InternalRow row : rows) {
                writer.write(row);
            }
            List<CommitMessage> messages = writer.prepareCommit();
            try (BatchTableCommit c = writeBuilder.newCommit()) {
                c.commit(messages);
            }
        }
    }

    public static void generate(String rootPath) throws Exception {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("val", DataTypes.STRING())
                .primaryKey("id")
                .option("bucket", "1")
                .option("file.format", "parquet")
                .build();

        Identifier tableId = Identifier.create("tests", "pk_t");

        Catalog catalog = createFilesystemCatalog(rootPath);
        catalog.createDatabase("tests", true);
        catalog.createTable(tableId, schema, false);

        // snapshot 1: (1, 'old'), (2, 'two')
        Table table = catalog.getTable(tableId);
        commit(table, List.of(
                GenericRow.of(1, BinaryString.fromString("old")),
                GenericRow.of(2, BinaryString.fromString("two"))));

        // snapshot 2: upsert (1, 'new') -- supersedes (1, 'old')
        table = catalog.getTable(tableId);
        commit(table, Collections.singletonList(
                GenericRow.of(1, BinaryString.fromString("new"))));
    }

    public static void main(String[] args) throws Exception {
        generate(args.length > 0 ? args[0] : "/tmp/paimon_pk_wh");
    }
}
```

4. Run
```
mvn -B compile exec:java -Dexec.args=/tmp/paimon_pk_wh
```

5. Copy the generated table directory into this location
```
cp -r /tmp/paimon_pk_wh/tests.db/pk_t/* tests/queries/0_stateless/data_minio/paimon_primary_key/
```

Final project tree
```
.
├── pom.xml
└── src
    └── main
        └── java
            └── gen
                └── PkDataGenerator.java
```
