import zipfile
import os

LOCAL_ZIP_DIR = "/tmp/mock_zip"
LOCAL_ZIP_PATH = f"{LOCAL_ZIP_DIR}/NEWRX_TEST.zip"

os.makedirs(LOCAL_ZIP_DIR, exist_ok=True)

with zipfile.ZipFile(LOCAL_ZIP_PATH, "w") as z:
    z.writestr("INEWRX_20250555.csv", "id,name\n1,Alice\n2,Bob")
    z.writestr("INEWRX_20250556.csv", "id,name\n3,Charlie\n4,David")

print("✅ Mock ZIP created locally:", LOCAL_ZIP_PATH)


DBFS_ZIP_DIR = "dbfs:/tmp/mock_zip"
DBFS_ZIP_PATH = f"{DBFS_ZIP_DIR}/NEWRX_TEST.zip"

dbutils.fs.mkdirs(DBFS_ZIP_DIR)
dbutils.fs.mv(f"file:{LOCAL_ZIP_PATH}", DBFS_ZIP_PATH, True)

print("✅ Mock ZIP copied to DBFS:", DBFS_ZIP_PATH)


dbutils.fs.ls("dbfs:/tmp/mock_zip")


EXTRACT_DIR = "/tmp/extracted"
os.makedirs(EXTRACT_DIR, exist_ok=True)

with zipfile.ZipFile("/dbfs/tmp/mock_zip/NEWRX_TEST.zip", "r") as zip_ref:
    zip_ref.extractall(EXTRACT_DIR)

print("✅ ZIP extracted to:", EXTRACT_DIR)
print(os.listdir(EXTRACT_DIR))


csv_df = spark.read.option("header", True).csv(
    "file:/tmp/extracted/*.csv"
)

csv_df.show(truncate=False)

