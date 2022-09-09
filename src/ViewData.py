# Databricks notebook source
import time 
from pyspark.sql.functions import *


# COMMAND ----------

dbutils.widgets.text("schema_name", "")

# COMMAND ----------

spark.sql("USE {}".format(dbutils.widgets.get("schema_name")))

# COMMAND ----------

display(
  spark.read.table("customer")
)

# COMMAND ----------

display(
  spark.read.table("customer_address")
)

# COMMAND ----------

display(
  spark.read.table("order_line_items")
)

# COMMAND ----------

display(
  spark.read.table("monthly_sales")
)

# COMMAND ----------

# DBTITLE 1,Show the old records in an scd2
display(
  spark.read.table("customer_address_scd2").filter(col("__END_AT").isNotNull())
)

# COMMAND ----------

# DBTITLE 1,Show the new records in an scd2
display(
  spark.read.table("customer_address_scd2").filter(col("__END_AT").isNull())
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

time.sleep(3600)
for s in spark.streams.active:
  s.stop()

# COMMAND ----------


