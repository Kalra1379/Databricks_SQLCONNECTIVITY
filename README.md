# ================================
# Directory Browser (Databricks)
# ================================

import json
import os

# Assume payload is already available from previous cell
values = payload.get("P_OUTPUT_CURSOR_FULL", {}).get("values", [])

# Extract first LOOKUP_VALUE (Directory)
directory = values[0]["LOOKUP_VALUE"] if values else None

if not directory:
    raise ValueError("No LOOKUP_VALUE found in P_OUTPUT_CURSOR_FULL")

print(f"Resolved directory for FULL: {directory}")

# Write directory to JSON so downstream tools (SnapLogic equivalent) can read
out_dir = "/dbfs/tmp"
out_file = os.path.join(out_dir, "directory_browser_output.json")

os.makedirs(out_dir, exist_ok=True)

with open(out_file, "w") as f:
    json.dump({"directory": directory}, f)

print(f"Wrote directory browser output to {out_file}")









# ================================
# Filter Snap (Databricks)
# ================================

from pyspark.sql import functions as F

# Convert cursor output to DataFrame
df = spark.createDataFrame(
    payload["P_OUTPUT_CURSOR_FULL"]["values"]
)

# Resolve LASTEXECUTIONDATE
df = df.withColumn(
    "LASTEXECUTIONDATE_RESOLVED",
    F.when(
        F.col("LASTEXECUTIONDATE").isNull(),
        F.date_sub(F.current_date(), 6)
    ).otherwise(F.to_date("LASTEXECUTIONDATE"))
)

# Apply filter condition (SnapLogic equivalent)
filtered_df = df.filter(
    F.to_date(F.col("UPDATE_DATE")) > F.col("LASTEXECUTIONDATE_RESOLVED")
)

# Show filtered result
filtered_df.show(truncate=False)
