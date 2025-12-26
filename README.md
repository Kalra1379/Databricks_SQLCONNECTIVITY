# JDBC connection details
jdbc_url = "jdbc:oracle:thin:@//host:1521/SERVICE_NAME"
username = "EPRESCRIBING"
password = "pre#snk"

# Access JVM
jvm = spark._sc._gateway.jvm

conn = None
stmt = None

try:
    # Create JDBC connection
    conn = jvm.java.sql.DriverManager.getConnection(
        jdbc_url,
        username,
        password
    )

    # Prepare callable statement
    stmt = conn.prepareCall(
        "{call EPRESCRIBING.SP_NEWRX_PROCESS_LOG(?, ?, ?, ?)}"
    )

    # Set parameters (ORDER MATTERS)
    stmt.setString(1, "PIPELINE STARTED")
    stmt.setString(2, "Registry Data Full Load")
    stmt.setString(3, "STARTED")
    stmt.setString(4, "Process Started Registry Data Full Load")

    # Execute procedure
    stmt.execute()

    print("✔ Stored procedure executed successfully")

except Exception as e:
    print("❌ Error executing stored procedure:", e)

finally:
    if stmt:
        stmt.close()
    if conn:
        conn.close()
