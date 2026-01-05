import zipfile
import os

mock_zip_path = "/dbfs/tmp/NEWRX_TEST.zip"
mock_extract_dir = "/dbfs/tmp/NEWRX_EXTRACTED"

os.makedirs(mock_extract_dir, exist_ok=True)

with zipfile.ZipFile(mock_zip_path, 'w') as z:
    z.writestr("INEWRX_20250555.csv", "id,name\n1,Alice\n2,Bob")
    z.writestr("INEWRX_20250556.csv", "id,name\n3,Charlie\n4,David")

print("Mock ZIP created:", mock_zip_path)



import zipfile

zip_path = "/dbfs/tmp/NEWRX_TEST.zip"
extract_path = "/dbfs/tmp/NEWRX_EXTRACTED"

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

print("Extracted files:")
print(os.listdir(extract_path))



csv_df = spark.read.option("header", True).csv(
    "dbfs:/tmp/NEWRX_EXTRACTED/*.csv"
)

csv_df.show(truncate=False)



zip_files = json_split_df.select("filePath").collect()

for row in zip_files:
    zip_path = "/dbfs" + row["filePath"]   # adapt later for SFTP
    extract_dir = "/dbfs/tmp/extracted_" + os.path.basename(zip_path)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    print("Extracted from:", zip_path)


    # Check zip exists
os.path.exists(mock_zip_path)

# Check extracted files
os.listdir("/dbfs/tmp/NEWRX_EXTRACTED")








Add SFTP credentials (Databricks Secrets – BEST PRACTICE)
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
