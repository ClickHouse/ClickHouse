## How this data is generated?

this data has two columns: `id` which is an integer and `name` which is a string.
we write 10000 records data first from `id=0` to `id=9999`, and `name` column is assigned as string `name_{0-9}`, then remove `id=10` by equality deletes, and `id < 20` by position deletes.
then, we remove the name with value `name_9`, and remove `id < 30`

to test sequencial number, we write some data from 1-10, again, and the old deletes should not effect these data.
then we remove id = 3 and id = 8 with 1 equality deletes file.

``` java
package com.example;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

public class IcebergInsert {
  public static void equality_deletes_id(SparkSession spark) throws Exception {
    Configuration hadoopConf =
        spark.sparkContext().hadoopConfiguration(); // get hadoop conf from Spark
    hadoopConf.set("fs.s3a.endpoint", "http://127.0.0.1:8555");
    hadoopConf.set("fs.s3a.access.key", "minioadmin");
    hadoopConf.set("fs.s3a.secret.key", "minioadmin");
    hadoopConf.set("fs.s3a.path.style.access", "true");
    HadoopCatalog catalog = new HadoopCatalog(hadoopConf, "s3a://test/");
    Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("deletes_db"), "eq_deletes_table"));

    Schema deleteSchema = table.schema().caseInsensitiveSelect("id");

    int idFieldId = table.schema().findField("id").fieldId();

    String filename = "eq-delete-" + System.currentTimeMillis() + ".parquet";
    String deletePath = table.locationProvider().newDataLocation(filename);
    OutputFile out = table.io().newOutputFile(deletePath);

    EqualityDeleteWriter<GenericRecord> eqWriter =
        Parquet.writeDeletes(out) // returns DeleteWriteBuilder
            .forTable(table) // associate with table (sets schema/spec)
            .equalityFieldIds(idFieldId) // which field ids are equality keys
            .rowSchema(deleteSchema) // the schema of rows written in delete file
            .withSpec(table.spec()) // partition spec so the file is placed correctly
            .createWriterFunc(
                parquetMessageType -> GenericParquetWriter.buildWriter(parquetMessageType))
            .buildEqualityWriter(); // build the EqualityDeleteWriter

    GenericRecord deleteRec = GenericRecord.create(deleteSchema);
    deleteRec.setField("id", 10); // delete where id == 10
    eqWriter.write(deleteRec);
    eqWriter.close();

    org.apache.iceberg.DeleteFile deleteFile = eqWriter.toDeleteFile();

    table.newRowDelta().addDeletes(deleteFile).commit();
  }

  public static void equality_deletes_id_name(SparkSession spark) throws Exception {
    Configuration hadoopConf =
        spark.sparkContext().hadoopConfiguration(); // get hadoop conf from Spark
    hadoopConf.set("fs.s3a.endpoint", "http://127.0.0.1:8555");
    hadoopConf.set("fs.s3a.access.key", "minioadmin");
    hadoopConf.set("fs.s3a.secret.key", "minioadmin");
    hadoopConf.set("fs.s3a.path.style.access", "true");
    HadoopCatalog catalog = new HadoopCatalog(hadoopConf, "s3a://test/");
    Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("deletes_db"), "eq_deletes_table"));

    Schema deleteSchema = table.schema();

    int idFieldId = table.schema().findField("id").fieldId();
    int nameFieldId = table.schema().findField("name").fieldId();

    String filename = "eq-delete-" + System.currentTimeMillis() + ".parquet";
    String deletePath = table.locationProvider().newDataLocation(filename);
    OutputFile out = table.io().newOutputFile(deletePath);

    EqualityDeleteWriter<GenericRecord> eqWriter =
        Parquet.writeDeletes(out) // returns DeleteWriteBuilder
            .forTable(table) // associate with table (sets schema/spec)
            .equalityFieldIds(new int[]{idFieldId, nameFieldId}) // which field ids are equality keys
            .rowSchema(deleteSchema) // the schema of rows written in delete file
            .withSpec(table.spec()) // partition spec so the file is placed correctly
            .createWriterFunc(
                parquetMessageType -> GenericParquetWriter.buildWriter(parquetMessageType))
            .buildEqualityWriter(); // build the EqualityDeleteWriter

    GenericRecord deleteRec = GenericRecord.create(deleteSchema);
    deleteRec.setField("id", 9);
    deleteRec.setField("name", "name_9");
    GenericRecord deleteRec1 = GenericRecord.create(deleteSchema);
    deleteRec1.setField("id", 3);
    deleteRec1.setField("name", "name_3");
    eqWriter.write(deleteRec);
    eqWriter.write(deleteRec1);
    eqWriter.close();

    org.apache.iceberg.DeleteFile deleteFile = eqWriter.toDeleteFile();

    table.newRowDelta().addDeletes(deleteFile).commit();
  }
  public static void equality_deletes_name(SparkSession spark) throws Exception {
    Configuration hadoopConf =
        spark.sparkContext().hadoopConfiguration(); // get hadoop conf from Spark
    hadoopConf.set("fs.s3a.endpoint", "http://127.0.0.1:8555");
    hadoopConf.set("fs.s3a.access.key", "minioadmin");
    hadoopConf.set("fs.s3a.secret.key", "minioadmin");
    hadoopConf.set("fs.s3a.path.style.access", "true");
    HadoopCatalog catalog = new HadoopCatalog(hadoopConf, "s3a://test/");
    Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("deletes_db"), "eq_deletes_table"));

    Schema deleteSchema = table.schema().caseInsensitiveSelect("name");

    int nameFieldId = table.schema().findField("name").fieldId();

    String filename = "eq-delete-" + System.currentTimeMillis() + ".parquet";
    String deletePath = table.locationProvider().newDataLocation(filename);
    OutputFile out = table.io().newOutputFile(deletePath);

    EqualityDeleteWriter<GenericRecord> eqWriter =
        Parquet.writeDeletes(out) // returns DeleteWriteBuilder
            .forTable(table) // associate with table (sets schema/spec)
            .equalityFieldIds(nameFieldId) // which field ids are equality keys
            .rowSchema(deleteSchema) // the schema of rows written in delete file
            .withSpec(table.spec()) // partition spec so the file is placed correctly
            .createWriterFunc(
                parquetMessageType -> GenericParquetWriter.buildWriter(parquetMessageType))
            .buildEqualityWriter(); // build the EqualityDeleteWriter

    GenericRecord deleteRec = GenericRecord.create(deleteSchema);
    deleteRec.setField("name", "name_9"); // delete where id == 10
    eqWriter.write(deleteRec);
    eqWriter.close();

    org.apache.iceberg.DeleteFile deleteFile = eqWriter.toDeleteFile();

    table.newRowDelta().addDeletes(deleteFile).commit();
  }

  public static void main(String[] args) throws Exception {
    SparkSession spark =
        SparkSession.builder()
            .appName("IcebergInsertJava")
            // no master here; supplied by spark-submit --master
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.my_catalog.type", "hadoop")
            .config("spark.sql.catalog.my_catalog.warehouse", "s3a://test/")
            // MinIO / S3A settings - overwritten with spark-submit --conf below if needed
            .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:8555")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .getOrCreate();

    // create namespace / database
    spark.sql("CREATE NAMESPACE IF NOT EXISTS my_catalog.deletes_db");

    spark.sql("DROP TABLE IF EXISTS my_catalog.deletes_db.eq_deletes_table");
    // create table if not exists
    spark.sql(
        "CREATE TABLE IF NOT EXISTS my_catalog.deletes_db.eq_deletes_table ("
            + "id INT, name STRING) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='2',"
            + "'write.delete.mode'='merge-on-read',"
            + "'write.update.mode'='merge-on-read',"
            + "'write.merge.mode'='merge-on-read',"
            + "'write.delete.granularity'='file'"
            + ")");

    // 3) Generate 1000 random rows and append
    int rowsToGenerate = 1000;
    List<Row> rows = new ArrayList<>(rowsToGenerate);
    for (int i = 0; i < rowsToGenerate; i++) {
      String name = "name_" + (i % 10);
      rows.add(RowFactory.create(i, name));
    }
    //
    // build small dataset and append
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("name", DataTypes.StringType, false, Metadata.empty())
            });

    spark.createDataFrame(rows, schema).writeTo("my_catalog.deletes_db.eq_deletes_table").append();

    equality_deletes_id(spark);
    spark.sql("DELETE FROM my_catalog.deletes_db.eq_deletes_table WHERE id < 20");
    equality_deletes_name(spark);
    spark.sql("DELETE FROM my_catalog.deletes_db.eq_deletes_table WHERE id < 30");
    // insert more data, test if we process sequence number correctly.
    rowsToGenerate = 10;
    rows = new ArrayList<>(rowsToGenerate);
    for (int i = 0; i < rowsToGenerate; i++) {
      String name = "name_" + (i % 10);
      rows.add(RowFactory.create(i, name));
    }

    spark.createDataFrame(rows, schema).writeTo("my_catalog.deletes_db.eq_deletes_table").append();
    equality_deletes_id_name(spark);
    spark.stop();
  }
}

```

## How is `eq_deletes_required_table` generated?

The table has a `data STRING NOT NULL` column (required in the Iceberg schema) and an
equality delete file in which the same column is optional, so the nullability of the
column differs between the delete file and the table schema. The delete file removes
`hello` and `foo`, leaving 2 of 4 rows.

Generated with the Iceberg Java API through the PySpark JVM gateway (Spark SQL alone
cannot write equality deletes), inside the `clickhouse/integration-tests-runner` image:

``` python
import pyspark

spark = (
    pyspark.sql.SparkSession.builder.appName("gen")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.spark_catalog.warehouse", "/out/warehouse")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .master("local")
    .getOrCreate()
)

TABLE = "eq_deletes_required_table"
spark.sql(
    f"CREATE TABLE {TABLE} (id INT NOT NULL, data STRING NOT NULL) USING iceberg"
    " TBLPROPERTIES ('format-version'='2')"
)
spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'hello'), (2, 'world'), (3, 'foo'), (4, 'bar')")

# Write an equality delete file whose `data` column is optional in the file schema,
# although it is required in the table schema.
jvm = spark.sparkContext._jvm
gateway = spark.sparkContext._gateway
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

table = jvm.org.apache.iceberg.hadoop.HadoopTables(hadoop_conf).load(f"/out/warehouse/default/{TABLE}")
schema = table.schema()
field_id = schema.findField("data").fieldId()
types = jvm.org.apache.iceberg.types.Types
delete_field = types.NestedField.optional(field_id, "data", types.StringType.get())
delete_schema = jvm.org.apache.iceberg.Schema([delete_field])

equality_ids = gateway.new_array(jvm.int, 1)
equality_ids[0] = field_id

appender_factory = jvm.org.apache.iceberg.data.GenericAppenderFactory(
    schema, table.spec(), equality_ids, delete_schema, None
)
out = table.io().newOutputFile(table.locationProvider().newDataLocation("eq-delete-required-optional.parquet"))
writer = appender_factory.newEqDeleteWriter(
    jvm.org.apache.iceberg.encryption.EncryptedFiles.plainAsEncryptedOutput(out),
    jvm.org.apache.iceberg.FileFormat.PARQUET,
    None,
)
for value in ["hello", "foo"]:
    record = jvm.org.apache.iceberg.data.GenericRecord.create(delete_schema)
    record.setField("data", value)
    writer.write(record)
writer.close()

table.newRowDelta().addDeletes(writer.toDeleteFile()).commit()
spark.stop()
```

The resulting `/out/warehouse/default/eq_deletes_required_table` directory is copied here
as is, except for the `.*.crc` files.

## How is `eq_deletes_null_key_table` generated?

The table has four optional columns `id INT, category STRING, name STRING, tag STRING` and one
Parquet data file:

| id | category | name | tag |
|---|---|---|---|
| 1 | marsupial | Koala | t |
| 2 | toy | Teddy | t |
| 3 | NULL | Grizzly | t |
| 4 | NULL | Polar | t |
| 4 | bear | Kodiak | t |
| NULL | toy | Ghost | t |
| NULL | NULL | Phantom | t |
| 5 | toy | Doomed | NULL |

Three Parquet equality delete files follow, one commit each: on `id` deleting `id = 2` (Teddy), on
`(id, category)` deleting `(4, NULL)` (Polar only), and on `tag` deleting `tag = NULL` (Doomed). The
Iceberg Java reader returns Ghost, Grizzly, Koala, Kodiak and Phantom.

Generated with the Iceberg Java API (`iceberg-spark-runtime-4.1_2.13-1.11.0`) through the PySpark JVM
gateway (Spark SQL cannot write equality deletes):

``` python
import glob
import os

from pyspark.sql import SparkSession

WAREHOUSE = "/tmp/iceberg_warehouse"
TABLE = "eq_deletes_null_key_table"

jar = glob.glob(os.path.expanduser("~/.ivy2.5.2/jars/org.apache.iceberg_iceberg-spark-runtime-4.1_2.13-1.11.0.jar"))[0]
spark = SparkSession.builder.master("local[1]").config("spark.jars", jar).getOrCreate()
jvm = spark._jvm
gateway = spark.sparkContext._gateway
ice = jvm.org.apache.iceberg
NestedField = getattr(ice.types, "Types$NestedField")
IntegerType = getattr(ice.types, "Types$IntegerType")
StringType = getattr(ice.types, "Types$StringType")
PARQUET = getattr(ice.FileFormat, "PARQUET")

catalog = ice.hadoop.HadoopCatalog(spark._jsc.hadoopConfiguration(), WAREHOUSE)
names = gateway.new_array(jvm.String, 2)
names[0] = "default"
names[1] = TABLE
ident = ice.catalog.TableIdentifier.of(names)

fields = jvm.java.util.ArrayList()
fields.add(NestedField.optional(1, "id", IntegerType.get()))
fields.add(NestedField.optional(2, "category", StringType.get()))
fields.add(NestedField.optional(3, "name", StringType.get()))
fields.add(NestedField.optional(4, "tag", StringType.get()))
props = jvm.java.util.HashMap()
props.put("format-version", "2")
table = catalog.createTable(ident, ice.Schema(fields), ice.PartitionSpec.unpartitioned(), props)


def out(name):
    path = table.locationProvider().newDataLocation(name)
    return ice.encryption.EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(path))


rows = [
    (1, "marsupial", "Koala", "t"),
    (2, "toy", "Teddy", "t"),
    (3, None, "Grizzly", "t"),
    (4, None, "Polar", "t"),
    (4, "bear", "Kodiak", "t"),
    (None, "toy", "Ghost", "t"),
    (None, None, "Phantom", "t"),
    (5, "toy", "Doomed", None),
]
data_writer = ice.data.GenericAppenderFactory(table.schema(), table.spec()).newDataWriter(out("data-0.parquet"), PARQUET, None)
for i, c, n, t in rows:
    record = ice.data.GenericRecord.create(table.schema())
    record.setField("id", i)
    record.setField("category", c)
    record.setField("name", n)
    record.setField("tag", t)
    data_writer.write(record)
data_writer.close()
table.newAppend().appendFile(data_writer.toDataFile()).commit()

# One RowDelta commit per equality delete file: (equality columns, file name, delete rows).
deletes = [
    (["id"], "eq-delete-id.parquet", [{"id": 2}]),
    (["id", "category"], "eq-delete-id-category.parquet", [{"id": 4, "category": None}]),
    (["tag"], "eq-delete-tag-null.parquet", [{"tag": None}]),
]
for cols, file_name, delete_rows in deletes:
    table.refresh()
    col_list = jvm.java.util.ArrayList()
    for c in cols:
        col_list.add(c)
    delete_schema = table.schema().select(col_list)
    equality_ids = gateway.new_array(jvm.int, len(cols))
    for k, c in enumerate(cols):
        equality_ids[k] = table.schema().findField(c).fieldId()
    factory = ice.data.GenericAppenderFactory(table.schema(), table.spec(), equality_ids, delete_schema, None)
    writer = factory.newEqDeleteWriter(out(file_name), PARQUET, None)
    for delete_row in delete_rows:
        record = ice.data.GenericRecord.create(delete_schema)
        for k, v in delete_row.items():
            record.setField(k, v)
        writer.write(record)
    writer.close()
    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit()

# The rows the Iceberg Java reader returns after applying the deletes (the test reference).
it = ice.data.IcebergGenerics.read(catalog.loadTable(ident)).build().iterator()
while it.hasNext():
    r = it.next()
    print(r.getField("id"), r.getField("category"), r.getField("name"), r.getField("tag"))
spark.stop()
```

The resulting `/tmp/iceberg_warehouse/default/eq_deletes_null_key_table` directory is copied here
as is, except for the `.*.crc` files.
