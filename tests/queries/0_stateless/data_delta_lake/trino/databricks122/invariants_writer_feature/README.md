Data generated using Databricks 12.2:
Using PySpark because Spark SQL doesn't support creating a table with column invariants.

```py
import pyspark.sql.types
from delta.tables import DeltaTable

schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField(
        "col_invariants", 
        dataType = pyspark.sql.types.IntegerType(), 
        nullable = False, 
        metadata = { "delta.invariants": "col_invariants < 3" } 
    )
])

table = DeltaTable.create(spark) \
    .tableName("test_invariants") \
    .addColumns(schema) \
    .location("s3://trino-ci-test/default/test_invariants") \
    .property("delta.feature.invariants", "supported") \
    .execute()

spark.createDataFrame([(1,)], schema=schema).write.saveAsTable(
    "test_invariants",
    mode="append",
    format="delta",
)
```
