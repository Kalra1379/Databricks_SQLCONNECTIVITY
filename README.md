import os
import zipfile

# -------- INPUT JSON (example from previous step) ----------
file_doc = {
    "Name": "drfirst_rcopia4_provider_location_daily_update_20260104.zip",
    "original": {
        "directory": "smb://louiswsts1221.rsc.humad.com/FtpRoot/Web_Drug_Info/FEnIntegration/Test/Arpit"
    }
}

# -------- CONFIG ----------
SMB_PREFIX = "smb://louiswsts1221.rsc.humad.com/FtpRoot"
DBFS_MOUNT = "/dbfs/mnt/smb"
EXTRACT_ROOT = "/dbfs/tmp/extracted"

# -------- BUILD PATHS ----------
zip_filename = file_doc["Name"]

dbfs_dir = file_doc["original"]["directory"].replace(
    SMB_PREFIX,
    DBFS_MOUNT
)

zip_path = f"{dbfs_dir}/{zip_filename}"

extract_dir = f"{EXTRACT_ROOT}/{zip_filename.replace('.zip','')}"
os.makedirs(extract_dir, exist_ok=True)

# -------- EXTRACT ZIP ----------
with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall(extract_dir)

print("ZIP extracted to:", extract_dir)
print("Extracted files:", os.listdir(extract_dir))

