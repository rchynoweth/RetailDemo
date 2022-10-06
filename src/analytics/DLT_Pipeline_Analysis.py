# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

pipeline_path = "/pipelines/007538bf-a382-41b7-b093-2dd6e78b5f6e"
table_name = "customer"

# COMMAND ----------

dbutils.fs.ls(pipeline_path)

# COMMAND ----------

dbutils.fs.ls(f"{pipeline_path}/system/events")

# COMMAND ----------

dbutils.fs.ls(f"{pipeline_path}/checkpoints/{table_name}/1/")

# COMMAND ----------

events_df = (spark.read.load(f"{pipeline_path}/system/events"))

# COMMAND ----------

display(events_df)

# COMMAND ----------

events_df.write.saveAsTable('dlt_analysis')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC details,
# MAGIC   details:flow_definition.output_dataset,
# MAGIC   details:flow_definition.input_datasets,
# MAGIC   details:flow_definition.flow_type,
# MAGIC   details:flow_definition.schema,
# MAGIC   details:flow_definition.explain_text,
# MAGIC   details:flow_definition
# MAGIC FROM dlt_analysis
# MAGIC ORDER BY timestamp

# COMMAND ----------


