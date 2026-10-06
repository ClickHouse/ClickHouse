## How to Generate This Paimon Directory with Deletion Vectors Turned Off

This directory holds the table of `../paimon_append_deletion_vectors` after `'deletion-vectors.enabled'` was turned
off without a full compaction, written by Spark. `INSERT (1, 'old'), (2, 'two')` and
`UPDATE ... SET val = 'new' WHERE id = 1` produce snapshots `1 APPEND`, `2 APPEND` and `3 COMPACT`, whose index manifest
holds a `DELETION_VECTORS` entry marking `(1, 'old')` as deleted. `ALTER TABLE ... SET TBLPROPERTIES
('deletion-vectors.enabled' = 'false')` creates `schema-1`, and `INSERT (3, 'three')` commits snapshot `4 APPEND` on
`schema-1` that still points to the same index manifest. Paimon 1.1.1 allows this change without
`deletion-vectors.modifiable`.

### Pre-Requirements
* Python 3 with `pyspark==3.5.9` (`pip install pyspark==3.5.9`)
* JDK: java 21
* Spark downloads `org.apache.paimon:paimon-spark-3.5:1.1.1` from Maven Central

### Generate steps
1. Create `gen_paimon_dv_disabled.py`
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

t = "paimon.tests.paimon_append_deletion_vectors_disabled"
spark.sql("CREATE DATABASE IF NOT EXISTS paimon.tests")
spark.sql(f"CREATE TABLE {t} (id INT, val STRING) "
          "TBLPROPERTIES ('deletion-vectors.enabled' = 'true', 'file.format' = 'parquet')")
spark.sql(f"INSERT INTO {t} VALUES (1, 'old'), (2, 'two')")
spark.sql(f"UPDATE {t} SET val = 'new' WHERE id = 1")
spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('deletion-vectors.enabled' = 'false')")
spark.sql(f"INSERT INTO {t} VALUES (3, 'three')")
spark.sql(f"SELECT * FROM {t} ORDER BY id, val").show()
spark.sql(f"SELECT snapshot_id, schema_id, commit_kind FROM paimon.tests.`paimon_append_deletion_vectors_disabled$snapshots`").show()
spark.stop()
```

2. Run
```
python3 gen_paimon_dv_disabled.py /tmp/paimon_dv_disabled_wh
```
Spark prints the snapshots `1 0 APPEND`, `2 0 APPEND`, `3 0 COMPACT` and `4 1 APPEND`.

3. Copy the generated table directory into this location
```
cp -r /tmp/paimon_dv_disabled_wh/tests.db/paimon_append_deletion_vectors_disabled/* tests/queries/0_stateless/data_minio/paimon_append_deletion_vectors_disabled/
```
