# Databricks notebook source
# MAGIC %md
# MAGIC # On Shelf Availability 
# MAGIC 
# MAGIC This implementation is leveraged using this [solution accelerator](https://www.databricks.com/solutions/accelerators/on-shelf-availability) available on the Databricks website. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Prep 
# MAGIC 
# MAGIC **Goal**: identify potential out of stock and on-shelf availability issues requiring further scrutiny by analyzing store inventory records. 
# MAGIC 
# MAGIC In order to accomplish our goal we will implement an out-of-stock and on-shelf availability solution. 
# MAGIC - Out-of-stock (OOS): when a retailer does not have enough inventory to meet consumer demand. Resulting in loss of customer confidence and revenue. 
# MAGIC - On-shelf availability (OSA): when inventory is available in the store but is not accessible to customers. For example, items are pushed back on the shelf and appear to be out-of-stock. 

# COMMAND ----------

# raw files are published to a user specific directory 
user_name = spark.sql("SELECT current_user()").collect()[0][0]
raw_files = "/Users/{}/retail_demo/raw".format(user_name)

# parameterize the notebook
dbutils.widgets.text("schema_name", "")
schema_name = dbutils.widgets.get("schema_name")

# create and set default database
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE {schema_name}")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.fs.mkdirs(f"/users/{user_name}/retail_demo")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/te/driver
# MAGIC wget https://raw.githubusercontent.com/tredenceofficial/OSA-Data/main/osa_raw_data.csv
# MAGIC wget https://raw.githubusercontent.com/tredenceofficial/OSA-Data/main/vendor_leadtime_info.csv
# MAGIC ls

# COMMAND ----------

# schema for inventory data
inventory_schema = StructType([
  StructField('date',DateType()),
  StructField('store_id',IntegerType()),
  StructField('sku',IntegerType()),
  StructField('product_category',StringType()),
  StructField('total_sales_units',IntegerType()),
  StructField('on_hand_inventory_units',IntegerType()),
  StructField('replenishment_units',IntegerType()),
  StructField('inventory_pipeline',IntegerType()),
  StructField('units_in_transit',IntegerType()),
  StructField('units_in_dc',IntegerType()),
  StructField('units_on_order',IntegerType()),
  StructField('units_under_promotion',IntegerType()),
  StructField('shelf_capacity',IntegerType())
  ])
 
# read inventory data and persist as delta table
display(
  spark
   .read
   .csv(
       '/databricks/driver/osa_raw_data.csv',
       header = True,
       schema = inventory_schema,
       dateFormat = 'yyyyMMdd'
       )
   )

# COMMAND ----------

dbutils.fs.ls("/databricks/")

# COMMAND ----------


