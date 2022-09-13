# Databricks notebook source
# MAGIC %md
# MAGIC # Building Dynamic DLT Pipelines in PySpark
# MAGIC 
# MAGIC 
# MAGIC Resources:
# MAGIC - [Slowly Changing Dimensions](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-cdc.html)
# MAGIC - [DLT Cookbook](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-cookbook.html) 

# COMMAND ----------

import datetime 
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

random.rand

# COMMAND ----------

tables = ['customer', 'store', 'customer_address', 'store_address', 'order', 'product', 'order_actions']

# COMMAND ----------

user_name = 'ryan.chynoweth@databricks.com' # spark.sql("SELECT current_user()").collect()[0][0]
print(user_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Tables 
# MAGIC 
# MAGIC Load many tables with the same function!! 

# COMMAND ----------

### 
# This creates append only tables for our bronze sources
# we can do further modeling in silver/gold layers 
###
def generate_tables(table):
  @dlt.table(
    name=table,
    comment="BRONZE: {}".format(table)
  )
  def create_table():
    return (
      spark.readStream.format('cloudfiles')
        .option('cloudFiles.format', 'json')
        .load('/Users/{}/dynamic_dlt/raw/{}/{}_*.json'.format(user_name, table, table))
        .withColumn('input_file', input_file_name())
        .withColumn("load_datetime", current_timestamp())
     )

# COMMAND ----------

for t in tables:
  generate_tables(t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detail tables

# COMMAND ----------

def my_gold_table():
  silver_one = dlt.read("silver_table_one")
  silver_two = dlt.read("silver_table_two")
  return ( 
     silver_one.join(silver_two, silver_one.id == silver_two.id, how="inner")
  )

# COMMAND ----------

@dlt.table(
  name="customer_details",
  comment="SILVER: customer_details - contains all history of customer information and customer addresses."
)
def customer_details():
  customer = dlt.read('customer')
  customer_address = dlt.read('customer_address')
  return (
    customer.join(customer_address, customer.customer_id == customer_address.customer_id, 'left').select(
      customer.customer_id,
      customer.first_name,
      customer.last_name,
      customer.is_member, 
      customer.member_number,
      customer_address.address_id,
      customer_address.city,
      customer_address.state,
      customer_address.zip_code
    )
  )

# COMMAND ----------

@dlt.table(
  name="store_details",
  comment="SILVER: store_details - contains all history of store information and store addresses."
)
def store_details():
  store = dlt.read('store')
  store_address = dlt.read('store_address')

  return (
    store.join(store_address, store.store_id == store_address.store_id, 'left').select(
      store.store_id,
      store.manager,
      store_address.address_id,
      store_address.city,
      store_address.state,
      store_address.zip_code
    )
  )

# COMMAND ----------

@dlt.table(
  name="members",
  comment="SILVER: members - all customers who are loyalty members."
)
def members():
  customer = dlt.read('customer')
  return (
    customer.filter(customer.is_member == 1)
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics Tables 

# COMMAND ----------

basket_schema = ArrayType(StructType([
  StructField("product_id", StringType()),
  StructField("qty", LongType()),
  StructField("total", DoubleType())  
  ])
)


@dlt.table(
  name="order_line_items",
  comment="SILVER: table containing a row for each line item in the order"
)
def order_line_items():
  line_items = dlt.read('order')
  return (
    line_items.withColumn("value", from_json(col("basket"), basket_schema))
      .withColumn("attr", explode("value"))
      .select("order_id", "attr.*")
) 

# COMMAND ----------

@dlt.table(
  name="monthly_sales",
  comment="SILVER: table containing aggregate monthly sales"
)
def monthly_sales():
  orders = dlt.read('order').withColumn("month", month('datetime')).withColumn("year", year("datetime"))
  line_items = dlt.read('order_line_items')
  
  return (
    line_items.join(orders, line_items.order_id == orders.order_id, 'inner').groupBy("month", "year").agg(sum("total").alias("total"), sum("qty").alias("total_items"))
    )

# COMMAND ----------

# DBTITLE 1,SCD Type 1 
# customer address
dlt.create_streaming_live_table("customer_address_scd1")

dlt.apply_changes(
  target = "customer_address_scd1",
  source = "customer_address", 
  keys = ["customer_id"], 
  sequence_by = col("created_date"),
  stored_as_scd_type = 1
)


# store address 
dlt.create_streaming_live_table("store_address_scd1")

dlt.apply_changes(
  target = "store_address_scd1",
  source = "store_address", 
  keys = ["store_id"], 
  sequence_by = col("created_date"),
  stored_as_scd_type = 1
)

# COMMAND ----------

# DBTITLE 1,SCD Type 2 
# customer address
dlt.create_streaming_live_table("customer_address_scd2")

dlt.apply_changes(
  target = "customer_address_scd2",
  source = "customer_address", 
  keys = ["customer_id"], 
  sequence_by = col("created_date"),
  stored_as_scd_type = 2
)


# store address
dlt.create_streaming_live_table("store_address_scd2")

dlt.apply_changes(
  target = "store_address_scd2",
  source = "store_address", 
  keys = ["store_id"], 
  sequence_by = col("created_date"),
  stored_as_scd_type = 2
)
