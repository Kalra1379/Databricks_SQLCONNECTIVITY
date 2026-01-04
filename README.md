from pyspark.sql.functions import col, from_unixtime, current_date, date_sub

filtered_files = files_df.withColumn(
    "last_modified_date",
    from_unixtime(col("last_modified")).cast("date")
).filter(
    col("last_modified_date") > date_sub(current_date(), 6)
)

filtered_files.show(truncate=False)



from pyspark.sql.functions import to_date

sorted_df = filtered_df.orderBy(
    to_date(col("UPDATE_DATE")).desc()
)


sorted_df.select(
    "LOOKUP_VALUE",
    "UPDATE_DATE",
    "LASTEXECUTIONDATE_RESOLVED"
).show(truncate=False)


head_df = sorted_df.limit(1)


head_df.show(truncate=False)



row_count = head_df.count()
print(f"Gate check - rows after Head: {row_count}")



if row_count > 0:
    print("Gate OPEN: Data available, proceeding...")
    gated_df = head_df
else:
    print("Gate CLOSED: No data available, stopping pipeline")
    gated_df = None





    # FILTER → SORT → HEAD → GATE already done

# ROUTER
has_name = (
    gated_df is not None and
    "Name" in gated_df.columns and
    gated_df.filter(gated_df.Name.isNotNull()).count() > 0
)

from datetime import datetime
is_sunday = datetime.today().weekday() == 6

if has_name:
    route = "output0"
elif (not has_name) and is_sunday:
    route = "output1"
else:
    route = None

print("Router selected:", route)





from datetime import datetime

# Check if Name exists and has data
has_name = (
    gated_df is not None and
    "Name" in gated_df.columns and
    gated_df.filter(gated_df.Name.isNotNull()).count() > 0
)

# Check if today is Sunday
is_sunday = datetime.today().weekday() == 6  # Monday=0, Sunday=6

print("Router debug:")
print("Has Name:", has_name)
print("Is Sunday:", is_sunday)

# Decide route
if has_name:
    route = "output0"
elif (not has_name) and is_sunday:
    route = "output1"
else:
    route = None

print("Router selected route:", route)




if route == "output0":
    print("Executing Output0 → JSON Splitter equivalent")

    # Spark DataFrames are already split row-wise
    json_split_df = gated_df

    json_split_df.show(truncate=False)




    from pyspark.sql.functions import explode

if route == "output0":
    print("Executing Output0 → JSON Splitter using explode")

    json_split_df = gated_df.select(
        explode("input0").alias("row")
    )

    json_split_df.show(truncate=False)

if route == "output1":
    print("Executing Output1 → Get Email Recipient")

    jdbc_url = "jdbc:oracle:thin:@//HOST:PORT/SERVICE"
    connection_props = {
        "user": "USERNAME",
        "password": "PASSWORD",
        "driver": "oracle.jdbc.driver.OracleDriver"
    }

    query = """
    (
      SELECT EMAIL
      FROM TABLE(EPRESCRIBING.SP_NEWRX_GET_LOOKUP('EMAIL_RECIPIENT'))
    )
    """

    email_df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=connection_props
    )

    email_df.show(truncate=False)
    
