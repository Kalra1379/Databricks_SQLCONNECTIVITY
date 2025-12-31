Log Pipeline Started
        ↓
Truncate Registry Data   (FULL load only)
        ↓
Get File Path (lookup from DB)
        ↓
Mapper (format lookup output)
        ↓
Directory Browser (S3 path)
        ↓
Gate (ignore empty)
        ↓
Router
   ├── output0 → Incremental load path
   └── output1 → Full load path



from datetime import datetime
import jaydebeapi

# ---------- DAY CHECK ----------
today = datetime.now()
is_weekend = today.weekday() >= 5

# ---------- DB CONNECTION ----------
conn = jaydebeapi.connect(
    "oracle.jdbc.driver.OracleDriver",
    "jdbc:oracle:thin:@//HOST:PORT/SERVICE",
    ["USER", "PWD"],
    "/dbfs/FileStore/jars/ojdbc8.jar"
)
cursor = conn.cursor()

# ---------- FULL LOAD PREP ----------
if not is_weekend:
    cursor.callproc("EPRESCRIBING.SP_NEWRX_TRUNCATE_REGISTRY_DATA")
    conn.commit()

# ---------- GET FILE PATH ----------
df_lookup = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//HOST:PORT/SERVICE") \
    .option("dbtable", "(SELECT * FROM LOOKUP_TABLE)") \
    .option("user", "USER") \
    .option("password", "PWD") \
    .load()

lookup_path = df_lookup.select("LOOKUP_VALUE").first()[0]

# ---------- DIRECTORY BROWSE ----------
files = dbutils.fs.ls(lookup_path)
files = [f.path for f in files if "full" in f.name]

# ---------- GATE ----------
if not files:
    dbutils.notebook.exit("NO_FILES_FOUND")

# ---------- ROUTER ----------
if not is_weekend:
    dbutils.notebook.run("/Pipelines/Full_Load", 0, {"path": lookup_path})
else:
    dbutils.notebook.run("/Pipelines/Incremental_Load", 0, {"path": lookup_path})

cursor.close()
conn.close()












from datetime import datetime
import jaydebeapi

# ---------- PIPELINE NAME ----------
pipeline_name = dbutils.notebook.entry_point.getDbutils() \
    .notebook().getContext().notebookPath().get()

# ---------- LOG PIPELINE START ----------
conn = jaydebeapi.connect(
    "oracle.jdbc.driver.OracleDriver",
    "jdbc:oracle:thin:@//HOST:PORT/SERVICE",
    ["USER", "PASSWORD"],
    "/dbfs/FileStore/jars/ojdbc8.jar"
)

cursor = conn.cursor()
cursor.callproc(
    "EPRESCRIBING.SP_NEWRX_PROCESS_LOG",
    [pipeline_name, "PIPELINE STARTED", "STARTED", "Process Started"]
)
conn.commit()
cursor.close()
conn.close()

# ---------- CONDITIONAL EXECUTION ----------
today = datetime.now()
is_weekend = today.weekday() >= 5

if not is_weekend:
    dbutils.notebook.run("/Pipelines/Full_Load", 0)
else:
    dbutils.notebook.run("/Pipelines/Incremental_Load", 0)













-------------call store procedure ------------





import jaydebeapi

conn = jaydebeapi.connect(
    "oracle.jdbc.driver.OracleDriver",
    jdbc_url,
    ["DB_USER", "DB_PASSWORD"],
    "/dbfs/FileStore/jars/ojdbc8.jar"
)

cursor = conn.cursor()

cursor.callproc(
    "EPRESCRIBING.SP_NEWRX_PROCESS_LOG",
    [
        pipeline_name,                     # PipelineName
        "PIPELINE STARTED",                # Status
        "STARTED",                         # ExecutionFlag
        "Process Started to send Data"     # Message
    ]
)

conn.commit()
cursor.close()
conn.close()













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














    def run_weekday_pipeline():
    pipeline_name = "PipelineName_Incremental"
    status = "PIPELINE STARTED"
    stage = "STARTED"
    message = "Process Started to send Data to Dr First"

    print("Weekday pipeline execution started")

    # Log start
    log_pipeline_status(pipeline_name, status, stage, message)

    # ---- Business logic goes here ----
    print("Running weekday data load logic...")






    def run_weekend_pipeline():
    pipeline_name = "PipelineName_Weekend"
    status = "PIPELINE STARTED"
    stage = "STARTED"
    message = "Weekend incremental process started"

    print("Weekend pipeline execution started")

    # Log start
    log_pipeline_status(pipeline_name, status, stage, message)

    # ---- Business logic goes here ----
    print("Running weekend maintenance logic...")








    day_of_week = datetime.now().weekday()
print("Day of week:", day_of_week)

# Saturday = 5, Sunday = 6
if day_of_week in (5, 6):
    run_weekend_pipeline()
else:
    run_weekday_pipeline()
**-

