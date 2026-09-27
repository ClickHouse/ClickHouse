## How to Generate This Paimon Deletion-Vector Directory

This directory holds an Apache Paimon **append-only table** (no primary key) with
`'deletion-vectors.enabled' = 'true'`, written by Spark. `INSERT (1, 'old'), (2, 'two')` writes one data file;
`UPDATE ... SET val = 'new' WHERE id = 1` then writes `(1, 'new')` to a second data file and commits a `COMPACT`
snapshot whose index manifest holds a `DELETION_VECTORS` entry marking row 0 of the first file, i.e. `(1, 'old')`,
as deleted. The correct answer is therefore `1 new / 2 two`, while the raw union of the data files is
`1 new / 1 old / 2 two`.

### Pre-Requirements
* Python 3 with `pyspark==3.5.9` (`pip install pyspark==3.5.9`)
* JDK: java 21
* Spark downloads `org.apache.paimon:paimon-spark-3.5:1.1.1` from Maven Central

### Generate steps
1. Create `gen_paimon_dv.py`
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

t = "paimon.tests.paimon_append_deletion_vectors"
spark.sql("CREATE DATABASE IF NOT EXISTS paimon.tests")
spark.sql(f"CREATE TABLE {t} (id INT, val STRING) "
          "TBLPROPERTIES ('deletion-vectors.enabled' = 'true', 'file.format' = 'parquet')")
spark.sql(f"INSERT INTO {t} VALUES (1, 'old'), (2, 'two')")
spark.sql(f"UPDATE {t} SET val = 'new' WHERE id = 1")
spark.sql(f"SELECT * FROM {t} ORDER BY id").show()
spark.sql(f"SELECT snapshot_id, commit_kind FROM paimon.tests.`paimon_append_deletion_vectors$snapshots`").show()
spark.stop()
```

2. Run
```
python3 gen_paimon_dv.py /tmp/paimon_dv_wh
```
Spark prints `1 new / 2 two` and the snapshots `1 APPEND`, `2 APPEND`, `3 COMPACT`.

3. Copy the generated table directory into this location
```
cp -r /tmp/paimon_dv_wh/tests.db/paimon_append_deletion_vectors/* tests/queries/0_stateless/data_minio/paimon_append_deletion_vectors/
```
