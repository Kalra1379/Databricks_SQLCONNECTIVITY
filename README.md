from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("LOOKUP_VALUE", StringType(), True),
    StructField("UPDATE_DATE", StringType(), True),
    StructField("LASTEXECUTIONDATE", StringType(), True)
])



df = spark.createDataFrame(
    payload["P_OUTPUT_CURSOR_FULL"]["values"],
    schema=schema
)

df = df.withColumn(
    "LASTEXECUTIONDATE_RESOLVED",
    F.when(
        F.col("LASTEXECUTIONDATE").isNull(),
        F.date_sub(F.current_date(), 6)
    ).otherwise(
        F.to_date("LASTEXECUTIONDATE")
    )
)

filtered_df = df.filter(
    F.to_date(F.col("UPDATE_DATE")) >
    F.col("LASTEXECUTIONDATE_RESOLVED")
)

filtered_df.show(truncate=False)
