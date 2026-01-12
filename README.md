from datetime import datetime

pipeline = dbutils.widgets.get("pipeline").strip().lower()
current_day = datetime.now().isoweekday()   # Monday=1 ... Sunday=7

print("Pipeline:", pipeline)
print("Today:", current_day)

if pipeline == "incremental_pipeline":
    if current_day == 7:
        dbutils.notebook.run("/Workspace/other_pipeline_notebook_0", 0)
    else:
        dbutils.notebook.run("/Workspace/other_pipeline_notebook_1", 0)

else:
    raise Exception("Unsupported pipeline in routing")
