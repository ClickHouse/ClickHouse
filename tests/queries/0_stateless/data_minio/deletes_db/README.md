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
