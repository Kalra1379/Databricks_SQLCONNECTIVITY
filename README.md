import paramiko
from pyspark.sql import Row

# ---- SFTP details ----
SFTP_HOST = "sftp01.drfirst.com"
SFTP_PORT = 22
SFTP_USER = "your_user"
SFTP_PASSWORD = "your_password"
SFTP_DIR = "/"   # or subdirectory

# ---- Connect ----
transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)

sftp = paramiko.SFTPClient.from_transport(transport)

# ---- List files ----
files = []
for f in sftp.listdir_attr(SFTP_DIR):
    files.append(Row(
        file_name=f.filename,
        file_path=f"{SFTP_DIR}/{f.filename}",
        last_modified=f.st_mtime
    ))

sftp.close()
transport.close()

# ---- Create DataFrame ----
files_df = spark.createDataFrame(files)

files_df.show(truncate=False)
