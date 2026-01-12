pipeline = dbutils.widgets.get("pipeline").strip().lower()
print("Pipeline received:", pipeline)

if pipeline == "incremental_pipeline":
    # Route to incremental pipeline notebook
    dbutils.notebook.run("/Workspace/other_pipeline_notebook_incremental", 0)
    
elif pipeline == "full_pipeline":
    # Example: route to full pipeline notebook
    dbutils.notebook.run("/Workspace/other_pipeline_notebook_full", 0)

else:
    raise Exception(f"Invalid pipeline name: {pipeline}")
