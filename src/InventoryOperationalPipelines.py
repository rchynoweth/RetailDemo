# Databricks notebook source
# MAGIC %md
# MAGIC # Real-time Inventory and Vendor Data 

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import random
import time
import datetime
import threading

# COMMAND ----------

dbutils.widgets.text("schema_name", "")
schema_name = dbutils.widgets.get("schema_name")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE {schema_name}"

# COMMAND ----------

user_name = spark.sql("SELECT current_user()").collect()[0][0]


raw_files = "/Users/{}/retail_demo/raw/data".format(user_name)
raw_ckpts = "/Users/{}/retail_demo/raw/raw_ckpts".format(user_name)
raw_schemas = "/Users/{}/retail_demo/raw/raw_schemas".format(user_name)
bronze_ckpts = "/Users/{}/retail_demo/bronze/bronze_ckpts".format(user_name)

# COMMAND ----------

# DBTITLE 1,Read the transactional sources
inventory_df = (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "json")
               .option("cloudFiles.schemaLocation", raw_schemas+"/inventory")
               .load("{}/inventory/*.json".format(raw_files))
              )

vendor_df = (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "json")
               .option("cloudFiles.schemaLocation", raw_schemas+"/vendor")
               .load("{}/vendor/*.json".format(raw_files))
              )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingesting the Operational Data as Streams

# COMMAND ----------

####
# Write all incoming JSON data as delta tables - append only mode (i.e. not updates/deletes on these tables)
####

(inventory_df.select("*", input_file_name().alias("source_file"))
  .writeStream
  .option("checkpointLocation", raw_ckpts+"/inventory")
  .toTable("ops_inventory")
)

(vendor_df.select("*", input_file_name().alias("source_file"))
  .writeStream
  .option("checkpointLocation", raw_ckpts+"/vendor")
  .toTable("ops_vendor")
)


# COMMAND ----------

####
# Create the ops_orders_main
# - this table will be a working table to track and process orders submitted by customers 
# - updates, deletes, inserts are allowed
####

spark.sql(""" 
  CREATE TABLE IF NOT EXISTS ops_bronze_inventory (
    date date,
    product_id string,
    store_id string,
    on_hand_inventory_units integer,
    replenishment_units integer,
    inventory_pipeline integer,
    units_in_transity integer,
    units_in_dc integer,
    units_on_order integer,
    units_under_promotion integer,
    shelf_capacity integer,
    total_sales_units integer -- join with sales data to obtain
  ) USING DELTA;
"""
)

# COMMAND ----------

#### 
# Read the append only data as a stream and write to the operational table defined above
####
inventory_bronze_df = spark.readStream.table("ops_inventory")

# COMMAND ----------

# ops_bronze_inventory
(
  ops_orders_df.filter(col("_rescued_data").isNull())
  .drop("_rescued_data", "basket")
  .writeStream
  .option("mergeSchema", True)
  .option('checkpointLocation', bronze_ckpts+"/ops_bronze_inventory")
  .toTable("ops_bronze_inventory")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Observation

# COMMAND ----------

# DBTITLE 1,Watch orders being filled in real-time
display(spark.readStream.option('ignoreChanges', True).table("ops_orders_main").filter(col("filled") == True))

# COMMAND ----------

# DBTITLE 1,Customers being notified of their order status
display(spark.readStream.option('ignoreChanges', True).table("ops_customer_notifications"))

# COMMAND ----------


