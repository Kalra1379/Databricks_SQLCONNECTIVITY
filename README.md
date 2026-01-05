import zipfile
import os

MOCK_ZIP_DIR = "/dbfs/tmp/mock_zip"
EXTRACT_DIR = "/dbfs/tmp/extracted"

os.makedirs(MOCK_ZIP_DIR, exist_ok=True)
os.makedirs(EXTRACT_DIR, exist_ok=True)

mock_zip_path = f"{MOCK_ZIP_DIR}/NEWRX_TEST.zip"

with zipfile.ZipFile(mock_zip_path, "w") as z:
    z.writestr("INEWRX_20250555.csv", "id,name\n1,Alice\n2,Bob")
    z.writestr("INEWRX_20250556.csv", "id,name\n3,Charlie\n4,David")

print("✅ Mock ZIP created:", mock_zip_path)


from pyspark.sql import Row

json_split_df = spark.createDataFrame([
    Row(
        Name="NEWRX_TEST.zip",
        original_directory="/tmp/mock_zip/"
    )
])

json_split_df.show(truncate=False)

import zipfile

for row in json_split_df.collect():
    zip_path = "/dbfs" + row.original_directory + row.Name

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(EXTRACT_DIR)

print("✅ ZIP extracted (MOCK)")
print(os.listdir(EXTRACT_DIR))


csv_df = spark.read.option("header", True).csv(
    "dbfs:/tmp/extracted/*.csv"
)

csv_df.show(truncate=False)

SCENARIO B (FUTURE): ✅ REAL SFTP ACCESS AVAILABLE
⚠️ ONLY this part changes
Everything else stays same.
1️⃣ Add SFTP credentials (Databricks Secrets – BEST PRACTICE)
Copy code
Python
SFTP_HOST = dbutils.secrets.get("sftp_scope", "host")
SFTP_USER = dbutils.secrets.get("sftp_scope", "username")
SFTP_PASS = dbutils.secrets.get("sftp_scope", "password")
SFTP_PORT = 22
2️⃣ Download ZIP from SFTP
Copy code
Python
import paramiko

SFTP_REMOTE_DIR = "/NEWRX/"
LOCAL_ZIP_DIR = "/dbfs/tmp/sftp_zip"
os.makedirs(LOCAL_ZIP_DIR, exist_ok=True)

transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
transport.connect(username=SFTP_USER, password=SFTP_PASS)

sftp = paramiko.SFTPClient.from_transport(transport)

remote_zip = SFTP_REMOTE_DIR + "NEWRX_TEST.zip"
local_zip = LOCAL_ZIP_DIR + "/NEWRX_TEST.zip"

sftp.get(remote_zip, local_zip)

sftp.close()
transport.close()

print("✅ ZIP downloaded from SFTP:", local_zip)
3️⃣ ZIPFILE READ (SAME CODE AS MOCK)
Copy code
Python
import zipfile

with zipfile.ZipFile(local_zip, "r") as zip_ref:
    zip_ref.extractall(EXTRACT_DIR)

print("✅ ZIP extracted from SFTP")
print(os.listdir(EXTRACT_DIR))
4️⃣ Read extracted CSV (UNCHANGED)
Copy code
Python
csv_df = spark.read.option("header", True).csv(
    "dbfs:/tmp/extracted/*.csv"
)

csv_df.show(truncate=False)
🟨 FINAL SWITCHABLE PIPELINE (BEST PRACTICE)
Copy code
Python
USE_SFTP = False   # change to True later

if USE_SFTP:
    print("🔐 Using REAL SFTP")
    zip_path = "/dbfs/tmp/sftp_zip/NEWRX_TEST.zip"
else:
    print("🧪 Using MOCK ZIP")
    zip_path = "/dbfs/tmp/mock_zip/NEWRX_TEST.zip"

with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall("/dbfs/tmp/extracted")
