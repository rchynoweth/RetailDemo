# Databricks notebook source
# MAGIC %md
# MAGIC # Real-time Order Fulfillment 

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

# COMMAND ----------

schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

spark.sql(f"USE {schema_name}")

# COMMAND ----------

user_name = spark.sql("SELECT current_user()").collect()[0][0]

raw_files = "/Users/{}/dynamic_dlt/raw".format(user_name)
raw_schemas = "/Users/{}/dynamic_dlt/raw_schemas".format(user_name)
raw_ckpts = "/Users/{}/dynamic_dlt/raw_ckpts".format(user_name)
ops_ckpts = "/Users/{}/dynamic_dlt/ops_ckpts".format(user_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ops_customer;
# MAGIC DROP TABLE IF EXISTS ops_store;
# MAGIC DROP TABLE IF EXISTS ops_store_address;
# MAGIC DROP TABLE IF EXISTS ops_customer_address; 
# MAGIC DROP TABLE IF EXISTS ops_order;
# MAGIC DROP TABLE IF EXISTS ops_product;
# MAGIC DROP TABLE IF EXISTS ops_orders_main;
# MAGIC DROP TABLE IF EXISTS ops_quarantine_orders;
# MAGIC DROP TABLE IF EXISTS ops_order_line_items;
# MAGIC DROP TABLE IF EXISTS ops_customer_notifications; 

# COMMAND ----------

# if we rerun this entire notebook then we need to reset all of our metadata 
# go to "Continue Generating Data" if you want to pick up where you left off 
# dbutils.fs.rm(raw_files, True)
dbutils.fs.rm(raw_ckpts, True)
dbutils.fs.rm(raw_schemas, True)
dbutils.fs.rm(ops_ckpts, True)


# dbutils.fs.mkdirs(raw_files.replace('/dbfs', ''))
dbutils.fs.mkdirs(raw_ckpts)
dbutils.fs.mkdirs(raw_schemas)
dbutils.fs.mkdirs(ops_ckpts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Lifecycle 
# MAGIC 
# MAGIC The lifecycle of an order is as follows: 
# MAGIC 1. Orders are routed to `ops_orders_main` 
# MAGIC 1. Once an order is filled then we route notification to `ops_customer_notifications`
# MAGIC 1. Orders with a bad schema are routed to `ops_quarantine_orders`
# MAGIC 1. Order items are parsed and stored in `ops_order_line_items`
# MAGIC 1. We manually update `ops_order_line_items`
# MAGIC 1. Using complete mode we compile aggregate data on orders where `order_filled == False`
# MAGIC 1. Merge the aggregate data into `ops_order_main`
# MAGIC 1. Once order is filled it is routed to the `ops_pickup_completed_orders` or `ops_delivery_completed_orders` table 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Please note that following:**  
# MAGIC Output Modes: 
# MAGIC - Complete Mode - The entire updated Result Table will be written to the external storage. It is up to the storage connector to decide how to handle writing of the entire table.  
# MAGIC - Append Mode (default) - Only the new rows appended in the Result Table since the last trigger will be written to the external storage. This is applicable only on the queries where existing rows in the Result Table are not expected to change. 
# MAGIC 
# MAGIC Ignore Updates and Deletes:
# MAGIC - `ignoreDeletes`: when true it will ignore all transactions that delete data.
# MAGIC - `ignoreChanges`: when true all updates to rows will be re-processed. This means that if **files** were rewritten then the entire file is processed, therefore, downstream consumers need to handle duplicates. Operations such as UPDATE, MERGE INTO, DELETE (within partitions), or OVERWRITE will trigger files to be rewritten. Deletes are not propagated downstream. ignoreChanges subsumes ignoreDeletes. Therefore if you use ignoreChanges, your stream will not be disrupted by either deletions or updates to the source table.

# COMMAND ----------

# DBTITLE 1,Read the transactional sources
customer_df = (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "json")
               .option("cloudFiles.schemaLocation", raw_schemas+"/customer")
               .load("{}/customer/*.json".format(raw_files))
              )

customer_address_df = (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "json")
               .option("cloudFiles.schemaLocation", raw_schemas+"/customer_address")
               .load("{}/customer_address/*.json".format(raw_files))
              )

order_df = (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "json")
               .option("cloudFiles.schemaLocation", raw_schemas+"/order")
               .load("{}/order/*.json".format(raw_files))
              )

product_df = (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "json")
               .option("cloudFiles.schemaLocation", raw_schemas+"/product")
               .load("{}/product/*.json".format(raw_files))
              )

store_df = (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "json")
               .option("cloudFiles.schemaLocation", raw_schemas+"/store")
               .load("{}/store/*.json".format(raw_files))
              )

store_address_df = (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "json")
               .option("cloudFiles.schemaLocation", raw_schemas+"/store_address")
               .load("{}/store_address/*.json".format(raw_files))
              )

order_actions_df = (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "json")
               .option("cloudFiles.schemaLocation", raw_schemas+"/order_actions")
               .load("{}/order_actions/*.json".format(raw_files))
              )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingesting the Operational Data as Streams

# COMMAND ----------

####
# Write all incoming JSON data as delta tables - append only mode (i.e. not updates/deletes on these tables)
####

(customer_df.select("*", input_file_name().alias("source_file"))
  .writeStream
  .option("checkpointLocation", raw_ckpts+"/customer")
  .toTable("ops_customer")
)

(customer_address_df.select("*", input_file_name().alias("source_file"))
  .writeStream
  .option("checkpointLocation", raw_ckpts+"/customer_address")
  .toTable("ops_customer_address")
)

(order_df.select("*", input_file_name().alias("source_file"))
  .writeStream
  .option("checkpointLocation", raw_ckpts+"/order")
  .toTable("ops_order")
)

(product_df.select("*", input_file_name().alias("source_file"))
  .writeStream
  .option("checkpointLocation", raw_ckpts+"/product")
  .toTable("ops_product")
)

(store_df.select("*", input_file_name().alias("source_file"))
  .writeStream
  .option("checkpointLocation", raw_ckpts+"/store")
  .toTable("ops_store")
)

(store_address_df.select("*", input_file_name().alias("source_file"))
  .writeStream
  .option("checkpointLocation", raw_ckpts+"/store_address")
  .toTable("ops_store_address")
)


(order_actions_df.select("*", input_file_name().alias("source_file"))
  .writeStream
  .option("checkpointLocation", raw_ckpts+"/order_actions")
  .toTable("order_actions")
)


# COMMAND ----------

####
# Define the basket schema  
# {'basket': [{'product_id': 'x', 'qty': y, 'total': z}, {'product_id': 'x', 'qty': y, 'total': z}, ...]}
####
basket_schema = ArrayType(StructType([
  StructField("product_id", StringType()),
  StructField("qty", LongType()),
  StructField("total", DoubleType())  
  ])
)

# COMMAND ----------

####
# Create the ops_orders_main
# - this table will be a working table to track and process orders submitted by customers 
# - updates, deletes, inserts are allowed
####

spark.sql(
  """ CREATE TABLE IF NOT EXISTS ops_orders_main (
          customer_id string,
          datetime string,
          order_id string,
          store_id string,
          source_file string,
          filled_cnt int,
          item_cnt int,
          modified_datetime timestamp,
          created_datetime timestamp,
          filled boolean GENERATED ALWAYS AS (item_cnt == filled_cnt)
      ) USING DELTA;
"""
)

# COMMAND ----------

#### 
# Read the append only data as a stream and write to the operational table defined above
####
ops_orders_df = spark.readStream.table("ops_order")

# COMMAND ----------

# ops_orders_main
(
  ops_orders_df.filter(col("_rescued_data").isNull())
  .withColumn("filled_cnt", lit(0))
  .withColumn("item_cnt", size(from_json(col("basket"), basket_schema))) 
  .withColumn("modified_datetime", current_timestamp())
  .withColumn("created_datetime", current_timestamp())
  .drop("_rescued_data", "basket")
  .writeStream
  .option("mergeSchema", True)
  .option('checkpointLocation', ops_ckpts+"/ops_orders_main")
  .toTable("ops_orders_main")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operational Fulfillment Tables

# COMMAND ----------

## ops_quarantine_orders 
# - all orders data that does not fit the defined schema 
(
  ops_orders_df.filter(col("_rescued_data").isNotNull())
  .writeStream
  .option('checkpointLocation', ops_ckpts+"/ops_quarantine_orders")
  .toTable("ops_quarantine_orders")
)


## ops_order_line_items
# - each item in the order is a row in this table 
# - unique key is order_id and product_id
(
  ops_orders_df.filter(col("_rescued_data").isNull())
  .withColumn("value", from_json(col("basket"), basket_schema))
  .withColumn("attr", explode("value"))
  .withColumn("filled", lit(False))
#   .withColumn("order_filled", lit(False)) # potentially use to filter the read on the merge
  .withColumn("modified_datetime", current_timestamp())
  .withColumn("created_datetime", current_timestamp())
  .select("customer_id", "order_id", "store_id", "attr.product_id", "attr.qty", "attr.total", "filled", "modified_datetime", "created_datetime")
  .writeStream
  .option('checkpointLocation', ops_ckpts+"/ops_order_line_items")
  .toTable("ops_order_line_items")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Notifications 

# COMMAND ----------

## Foreach batch function
# used to insert data that results in a customer notification 
# The merge makes sure we do not insert duplicate records 
def merge_customer_notifications(microBatchDF, batchId):
  microBatchDF = microBatchDF.withColumn("batchId", lit(batchId))
  mytable = DeltaTable.forName(spark, 'ops_customer_notifications')
  
  (
    mytable.alias('target')
    .merge(microBatchDF.alias('source'), 
          "target.order_id = source.order_id"
          )
    .whenNotMatchedInsertAll().execute()
  )

# COMMAND ----------

## ops_customer_notifications
# writes a notification message when an order is filled 
(
  spark.readStream
  .option('ignoreChanges', True)
  .table("ops_orders_main")
  .filter(col("filled") == True)
  .distinct()
  .withColumn("created_datetime", current_timestamp())
  .withColumn("message", lit("Hello! All the items in your order have been picked."))
  .writeStream
  .option('checkpointLocation', ops_ckpts+"/ops_customer_notifications")
  .toTable("ops_customer_notifications")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order Status 

# COMMAND ----------

## foreach batch merge function 
# used to update an order status when one or many items are filled in an order 
def merge_order_status(microBatchDF, batchId):
  microBatchDF = microBatchDF.withColumn("batchId", lit(batchId))
  mytable = DeltaTable.forName(spark, 'ops_orders_main')
  
  (
    mytable.alias('target')
    .merge(microBatchDF.alias('source'), 
          "target.order_id = source.order_id"
          )
    .whenMatchedUpdate(set = 
          {
            "filled_cnt": "source.count",
            "modified_datetime": current_timestamp()
          }
      ).execute()
  )

# COMMAND ----------

## Updates ops_order_main with the number of line items that have been filled
# - maintains order state/status
# - when implemented `order_filled` can be used to filter the line items and reduce the size 

(
  spark.readStream
  .option('ignoreChanges', True)
  .table('ops_order_line_items')
  .filter(col('filled') == True)
  .distinct()
  .select("order_id")
  .groupBy("order_id")
  .count()
  .writeStream
  .option("checkpointLocation", ops_ckpts+"/ops_orders_main_line_item_merge")
  .outputMode("complete")
  .foreachBatch(merge_order_status)
  .start()
     )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Completed Orders Tables
# MAGIC 
# MAGIC `ops_pickup_completed_orders` and `ops_delivery_completed_orders`
# MAGIC 
# MAGIC 
# MAGIC Please notice the `txnVersion` and `txnAppId` used to ensure that we do not write duplicates and the `.cache()` to avoid scanning the data twice for both streams. 

# COMMAND ----------

## foreach batch merge function 
# used to update an order status when one or many items are filled in an order 
def write_completed_order(microBatchDF, batchId):
  microBatchDF.cache() # Scan data only once 
  
  df = microBatchDF.withColumn("batchId", lit(batchId))
  
  app_id = '8080'
  
  (df.filter(col("order_type") == "pickup")
   .write
   .option("txnVersion", batchId)
   .option("txnAppId", app_id)
   .mode("append")
   .saveAsTable('ops_pickup_completed_orders')
  )
  
  (df.filter(col("order_type") == "delivery")
   .write
   .option("txnVersion", batchId)
   .option("txnAppId", app_id)
   .mode("append")
   .saveAsTable('ops_delivery_completed_orders')
  )
  
  df.unpersist()
  
# Write to both the pickup and delivery completed orders table 
(
  spark.readStream
  .option('ignoreChanges', True)
  .table('ops_orders_main')
  .filter(col('filled') == True)
  .withColumn("transaction_completed", lit(False))
  .writeStream
  .option("checkpointLocation", ops_ckpts+"/ops_completed_orders")
  .foreachBatch(write_completed_order)
  .start()
)



#### Original 

# ## ops_pickup_completed_orders 
# # - route completed pickup orders to a table
# (
#   spark.readStream.option('ignoreChanges', True)
#   .table("ops_orders_main")
#   .filter(col("filled") == True)
#   .filter(col("order_type") == "pickup")
#   .withColumn("transaction_completed", lit(False))
#   .writeStream
#   .option("checkpointLocation", ops_ckpts+"/ops_pickup_completed_orders")
#   .toTable('ops_pickup_completed_orders')
# )


# ## ops_delivery_completed_orders
# # - route completed delivery orders to a table 
# (
#   spark.readStream.option('ignoreChanges', True)
#   .table("ops_orders_main")
#   .filter(col("filled") == True)
#   .filter(col("order_type") == "delivery")
#   .withColumn("transaction_completed", lit(False))
#   .writeStream
#   .option("checkpointLocation", ops_ckpts+"/ops_delivery_completed_orders")
#   .toTable('ops_delivery_completed_orders')
# )

# COMMAND ----------


## ops_customer_notifications
# - writes to customer notification table ("append only") when the order has been picked up by the customer
# - receipt is sent to customer
(
  spark.readStream
  .option('ignoreChanges', True)
  .table("ops_pickup_completed_orders")
  .filter(col("transaction_completed") == True)
  .distinct()
  .withColumn("created_datetime", current_timestamp())
  .withColumn("message", lit("Hello! Your pickup order has been completed. Please review your receipt."))
  .writeStream
  .option('checkpointLocation', ops_ckpts+"/ops_customer_completed_pickup_transactions_notifications")
  .toTable("ops_customer_notifications")
)


## ops_customer_notifications
# - writes a notification when the order has been delivered to the customer 
# - receipt is sent to customer
(
  spark.readStream
  .option('ignoreChanges', True)
  .table("ops_delivery_completed_orders")
  .filter(col("transaction_completed") == True)
  .distinct()
  .withColumn("created_datetime", current_timestamp())
  .withColumn("message", lit("Hello! Your delivery order has been completed. Please review your receipt."))
  .writeStream
  .option('checkpointLocation', ops_ckpts+"/ops_customer_completed_delivery_transactions_notifications")
  .toTable("ops_customer_notifications")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate Data Processing 
# MAGIC 
# MAGIC i.e. the simulates human actions and work from the store employess. The generate data notebook simulates actions taken on behalf of the customer. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order Fulfillment 

# COMMAND ----------

## function used to fulfill orders one at a time 
# - when executed this function will run for 1 hour 
# - all items in an order are filled at the same time
def fulfill_order():
  end_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=60)
  
  while datetime.datetime.utcnow() < end_time:
    order_ids = [r.order_id for r in spark.read.table("ops_orders_main").filter(col('filled') == False).select("order_id").limit(40).collect()]
    oid = order_ids[random.randint(0,len(order_ids)-1)]
    spark.sql("UPDATE ops_order_line_items SET filled = True where order_id = '{}' ".format(oid))


# COMMAND ----------

# execute thread
x = threading.Thread(target=fulfill_order)
x.start()


# COMMAND ----------

## Function used to mark and order comlpeted to the customer 
# - this is a delivery driver marking complete or a pickup employee putting bags into a car 
def complete_order():
  end_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=60)
  
  while datetime.datetime.utcnow() < end_time:
    order_ids = oid = [(r.order_id, r.order_type) for r in  spark.sql("SELECT order_id, order_type FROM ops_orders_main WHERE filled = TRUE and transaction_completed = FALSE LIMIT 10").collect()]
    oid = order_ids[random.randint(0,len(order_ids)-1)]
    spark.sql("UPDATE ops_{}_completed_orders SET transaction_completed = True where order_id = '{}' ".format(oid[1], oid[0]))
    spark.sql("UPDATE ops_{}_completed_orders SET transaction_completed = True where order_id = '{}' ".format(oid[1], oid[0]))


# COMMAND ----------

# execute thread
y = threading.Thread(target=complete_order)
y.start()

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


