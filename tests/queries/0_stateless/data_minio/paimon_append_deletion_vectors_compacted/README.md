## How to Generate This Paimon Directory with an Empty Deletion Vectors Index

This directory holds a bucketed Apache Paimon **append-only table** (`'bucket' = '1'`, no primary key) with
`'deletion-vectors.enabled' = 'true'`, written by Spark. `INSERT (1, 'old'), (2, 'two')` and
`UPDATE ... SET val = 'new' WHERE id = 1` commit snapshot `3 COMPACT`, whose index manifest holds a `DELETION_VECTORS`
entry with one deletion vector. A full compaction then rewrites the data files and commits snapshot `4 COMPACT`, whose
index manifest holds a `DELETION_VECTORS` entry with `_ROW_COUNT` 0: an empty index file that replaces the old one.
The correct answer is `1 new / 2 two`.

### Pre-Requirements
* Python 3 with `pyspark==3.5.9` (`pip install pyspark==3.5.9`)
* JDK: java 21
* Spark downloads `org.apache.paimon:paimon-spark-3.5:1.1.1` from Maven Central

### Generate steps
1. Create `gen_paimon_dv_compacted.py`
```
import sys
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.jars.packages", "org.apache.paimon:paimon-spark-3.5:1.1.1")
    .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
    .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
    .config("spark.sql.catalog.paimon.warehouse", "file:" + sys.argv[1])
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)

t = "paimon.tests.paimon_append_deletion_vectors_compacted"
spark.sql("CREATE DATABASE IF NOT EXISTS paimon.tests")
spark.sql(f"CREATE TABLE {t} (id INT, val STRING) "
          "TBLPROPERTIES ('deletion-vectors.enabled' = 'true', 'bucket' = '1', 'bucket-key' = 'id', 'file.format' = 'parquet')")
spark.sql(f"INSERT INTO {t} VALUES (1, 'old'), (2, 'two')")
spark.sql(f"UPDATE {t} SET val = 'new' WHERE id = 1")
spark.sql("CALL paimon.sys.compact(table => 'tests.paimon_append_deletion_vectors_compacted', compact_strategy => 'full')")
spark.sql(f"SELECT * FROM {t} ORDER BY id, val").show()
spark.sql(f"SELECT snapshot_id, commit_kind FROM paimon.tests.`paimon_append_deletion_vectors_compacted$snapshots`").show()
spark.stop()
```

2. Run
```
python3 gen_paimon_dv_compacted.py /tmp/paimon_dv_compacted_wh
```
Spark prints `1 new / 2 two` and the snapshots `1 APPEND`, `2 APPEND`, `3 COMPACT` and `4 COMPACT`.

3. Copy the generated table directory into this location
```
cp -r /tmp/paimon_dv_compacted_wh/tests.db/paimon_append_deletion_vectors_compacted/* tests/queries/0_stateless/data_minio/paimon_append_deletion_vectors_compacted/
```
