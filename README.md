import os
import zipfile

file_doc = {
    "Name": "drfirst_rcopia4_provider_location_full_20260104.zip",
    "original": {
        "directory": "smb://louiswsts1221.rsc.humad.com/FtpRoot/Web_Drug_Info/FEnIntegration/Test/Arpit"
    }
}

SMB_PREFIX = "smb://louiswsts1221.rsc.humad.com/FtpRoot"
DBFS_MOUNT = "/dbfs/mnt/smb"
EXTRACT_ROOT = "/dbfs/tmp/extracted"

zip_filename = file_doc["Name"]

dbfs_dir = file_doc["original"]["directory"].replace(
    SMB_PREFIX,
    DBFS_MOUNT
)

zip_path = f"{dbfs_dir}/{zip_filename}"

extract_dir = f"{EXTRACT_ROOT}/{zip_filename.replace('.zip','')}"
os.makedirs(extract_dir, exist_ok=True)

with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall(extract_dir)

print("ZIP extracted to:", extract_dir)
print("Extracted files:", os.listdir(extract_dir))
