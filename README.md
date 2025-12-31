def log_pipeline_status(pipeline_name, status, stage, message):
    conn = jaydebeapi.connect(
        "oracle.jdbc.driver.OracleDriver",
        jdbc_url,
        [oracle_user, oracle_password],
        ojdbc_jar_path
    )

    cursor = conn.cursor()

    cursor.callproc(
        "EPRESCRIBING.SP_NEWRX_PROCESS_LOG",
        [pipeline_name, status, stage, message]
    )

    cursor.close()
    conn.close()

    print("Logged:", pipeline_name, status)   


    def truncate_registry_data():
    conn = jaydebeapi.connect(
        "oracle.jdbc.driver.OracleDriver",
        jdbc_url,
        [oracle_user, oracle_password],
        ojdbc_jar
    )

    cursor = conn.cursor()

    cursor.callproc(
        "EPRESCRIBING.SP_NEWRX_TRUNCATE_REGISTRY_DATA"
    )

    conn.commit()
    cursor.close()
    conn.close()

    print("Registry data truncated successfully")
