# -----------------------------
# DB credentials
# -----------------------------
username = "EPRESCRIBING"
password = "presrik"
dw_server = "ISB1D003.humana.com"
port = "1810"
database_name = "IDDDEV"

proc_name = "EPRESCRIBING.SP_NEW_RX_PROCESS_LOG"

pipeline_name = "PIPELINE_STARTED"
status = "STARTED"
description = "Process Started Registry Data Full Load"

# -----------------------------
# JDBC URL (CORRECT)
# -----------------------------
jdbc_url = f"jdbc:oracle:thin:@//{dw_server}:{port}/{database_name}"

# -----------------------------
# JVM access
# -----------------------------
jvm = spark._sc._gateway.jvm
DriverManager = jvm.java.sql.DriverManager
Types = jvm.java.sql.Types

# -----------------------------
# Create connection
# -----------------------------
conn = DriverManager.getConnection(jdbc_url, username, password)

# -----------------------------
# Prepare callable statement
# -----------------------------
call = conn.prepareCall("{call " + proc_name + "(?, ?, ?)}")

call.setString(1, pipeline_name)
call.setString(2, status)
call.setString(3, description)

# -----------------------------
# Execute
# -----------------------------
call.execute()

# -----------------------------
# Close resources
# -----------------------------
call.close()
conn.close()

print("Stored Procedure executed successfully")
