from smb.SMBConnection import SMBConnection
import socket
import json
from datetime import datetime

# Connection details
server_name = "louiswsts1221"
server_ip = "louiswsts1221.rsc.humad.com"
share_name = "FtpRoot"

username = "Humana_prod_ftp_user"
password = "YOUR_PASSWORD"
client_machine_name = socket.gethostname()

# Connect
conn = SMBConnection(
    username,
    password,
    client_machine_name,
    server_name,
    use_ntlm_v2=True
)

conn.connect(server_ip, 445)

# Directory path
directory_path = "Web_Drug_Info/FEnIntegration/Test/Arpit"

# List directory
files = conn.listPath(share_name, directory_path)

# Convert to JSON
output = []

for f in files:
    if f.filename not in [".", ".."]:
        output.append({
            "Name": f.filename,
            "Path": f"smb://{server_ip}/{share_name}/{directory_path}/{f.filename}",
            "Type": "directory" if f.isDirectory else "file",
            "Size": f.file_size,
            "LastModified": datetime.fromtimestamp(
                f.last_write_time
            ).strftime("%Y-%m-%d %H:%M:%S")
        })

# Print JSON
print(json.dumps(output, indent=2))
