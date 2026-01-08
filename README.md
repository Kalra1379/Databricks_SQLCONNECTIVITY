import smbclient
import zipfile

smbclient.register_session("server", username="u", password="p")

with smbclient.open_file("smb://server/path/file.zip", "rb") as f:
    with open("/dbfs/tmp/file.zip", "wb") as out:
        out.write(f.read())

with zipfile.ZipFile("/dbfs/tmp/file.zip") as z:
    z.extractall("/dbfs/tmp/unzipped")
