temp_output_dir = "/dbfs/tmp/file_writer_output"

(
    csv_df
    .coalesce(1)                     # single output file
    .write
    .mode("overwrite")               # OVERWRITE (SnapLogic)
    .option("header", True)
    .csv(temp_output_dir)
)


import os
import shutil

final_output_path = f"{BASE_OUTPUT_PATH}/{CONTENT_LOCATION}"

# Find Spark-generated CSV
files = os.listdir(temp_output_dir)
part_file = [f for f in files if f.startswith("part-") and f.endswith(".csv")][0]

# Ensure output directory exists
os.makedirs(os.path.dirname(final_output_path), exist_ok=True)

# Move + rename
shutil.move(
    f"{temp_output_dir}/{part_file}",
    final_output_path
)

print("✅ File written to:", final_output_path)
