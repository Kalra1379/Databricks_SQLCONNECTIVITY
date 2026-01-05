from pyspark.sql import Row

directory_browser_df = spark.createDataFrame([
    Row(
        fileName="NEWRX_20250801.csv",
        filePath="/NEWRX_20250801.csv",
        lastModified="2025-08-01"
    ),
    Row(
        fileName="NEWRX_20250731.csv",
        filePath="/NEWRX_20250731.csv",
        lastModified="2025-07-31"
    )
])

directory_browser_df.show(truncate=False)

from pyspark.sql.functions import to_date, current_date, date_sub

filtered_df = directory_browser_df.filter(
    to_date("lastModified") > date_sub(current_date(), 6)
)



sorted_df = filtered_df.orderBy(
    to_date("lastModified").desc()
)


head_df = sorted_df.limit(1)
head_df.show(truncate=False)


has_files = head_df.count() > 0

from datetime import datetime
is_sunday = datetime.today().weekday() == 6

if has_files:
    route = "output0"
elif (not has_files) and is_sunday:
    route = "output1"
else:
    route = None

print("Router selected:", route)
