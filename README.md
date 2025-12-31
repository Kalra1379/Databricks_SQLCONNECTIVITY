# ---------------------------------------------
# 1. JDBC Connection details
# ---------------------------------------------
jdbc_url = "jdbc:oracle:thin:@//ISRIDP03.humana.com:1810/IDDDEV"

username = "EPRESCRIBING"
password = "Pr#srlk"

driver = "oracle.jdbc.driver.OracleDriver"


# ---------------------------------------------
# 2. Get Java JDBC connection from Spark
# ---------------------------------------------
java_import(spark._sc._gateway.jvm, "java.sql.DriverManager")

conn = spark._sc._gateway.jvm.DriverManager.getConnection(
    jdbc_url,
    username,
    password
)


# ---------------------------------------------
# 3. Prepare Stored Procedure call
# ---------------------------------------------
call_stmt = conn.prepareCall(
    "{call EPRESCRIBING.SP_NEWRXL_PROCESS_LOG(?, ?, ?, ?)}"
)

# ---------------------------------------------
# 4. Set Stored Procedure parameters
# ---------------------------------------------
call_stmt.setString(1, "_PipelineName_Incremental")
call_stmt.setString(2, "PIPELINE STARTED")
call_stmt.setString(3, "STARTED")
call_stmt.setString(4, "Process Started to send Data to Dr First")

# ---------------------------------------------
# 5. Execute Stored Procedure
# ---------------------------------------------
call_stmt.execute()

# ---------------------------------------------
# 6. Close resources
# ---------------------------------------------
call_stmt.close()
conn.close()

print("Stored Procedure executed successfully")
