previous_output = []

for f in files:
    if f.filename not in [".", ".."]:
        previous_output.append({
            "Name": f.filename,
            "Path": f"smb://{server_ip}/{share_name}/{directory_path}/{f.filename}",
            "Type": "directory" if f.isDirectory else "file",
            "Size": f.file_size,
            "LastModified": datetime.fromtimestamp(
                f.last_write_time
            ).strftime("%Y-%m-%d %H:%M:%S")
        })


  gate_output = {
    "input0": []
}

for item in previous_output:
    gate_output["input0"].append({
        **item,
        "original": {
            "directory": "smb://louiswsts1221.rsc.humad.com/FtpRoot/Web_Drug_Info/FEnIntegration/Test/Arpit"
        }
    })     

    import json
print(json.dumps(gate_output, indent=2))
