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
import uuid
import json

# COMMAND ----------

# %sql
# drop database if exists rac_retail_demo cascade 

# COMMAND ----------

dbutils.widgets.text("schema_name", "")
schema_name = dbutils.widgets.get("schema_name")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE {schema_name}")

# COMMAND ----------

user_name = spark.sql("SELECT current_user()").collect()[0][0]


raw_files = "/Users/{}/retail_demo/raw/data".format(user_name)
raw_ckpts = "/Users/{}/retail_demo/raw/raw_ckpts".format(user_name)
raw_schemas = "/Users/{}/retail_demo/raw/raw_schemas".format(user_name)
bronze_ckpts = "/Users/{}/retail_demo/bronze/bronze_ckpts".format(user_name)

# COMMAND ----------

# dbutils.fs.rm(raw_ckpts, True)
# dbutils.fs.rm(raw_schemas, True)
# dbutils.fs.rm(bronze_ckpts, True)

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
def read_ingest_json_data(table_name):
  return (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "json")
               .option("cloudFiles.schemaLocation", raw_schemas+"/"+table_name)
               .load("{}/{}/*.json".format(raw_files, table_name))
              )


customer_df = read_ingest_json_data('customer')
customer_address_df = read_ingest_json_data('customer_address')
order_df = read_ingest_json_data('order')
product_df = read_ingest_json_data('product')
store_df = read_ingest_json_data('store')
store_address_df = read_ingest_json_data('store_address')
order_actions_df = read_ingest_json_data('order_actions')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingesting the Operational Data as Streams

# COMMAND ----------

####
# Write all incoming JSON data as delta tables - append only mode (i.e. not updates/deletes on these tables)
####

def write_ingest_json_data(df, table_name):
  (df.select("*", input_file_name().alias("source_file"))
  .writeStream
  .option("checkpointLocation", raw_ckpts+"/"+table_name)
  .toTable(table_name)
    )
  
write_ingest_json_data(customer_df, 'ops_customer')
write_ingest_json_data(customer_address_df, 'ops_customer_address')
write_ingest_json_data(order_df, 'ops_order')
write_ingest_json_data(product_df, 'ops_product')
write_ingest_json_data(store_df, 'ops_store')
write_ingest_json_data(store_address_df, 'ops_store_address')
write_ingest_json_data(order_actions_df, 'ops_order_actions')

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
          filled boolean GENERATED ALWAYS AS (item_cnt == filled_cnt and item_cnt>0)
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
  .option('checkpointLocation', bronze_ckpts+"/ops_orders_main")
  .toTable("ops_orders_main")
)

# COMMAND ----------

display(spark.read.table("ops_orders_main").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operational Fulfillment Tables

# COMMAND ----------

## ops_quarantine_orders 
# - all orders data that does not fit the defined schema 
bad_order_strm = (
  ops_orders_df.filter(col("_rescued_data").isNotNull())
  .writeStream
  .option('checkpointLocation', bronze_ckpts+"/ops_quarantine_orders")
  .toTable("ops_quarantine_orders")
)




# COMMAND ----------

## ops_order_line_items
# - each item in the order is a row in this table 
# - unique key is order_id and product_id
li_strm = (
  ops_orders_df.filter(col("_rescued_data").isNull())
  .withColumn("value", from_json(col("basket"), basket_schema))
  .withColumn("attr", explode("value"))
  .withColumn("filled", lit(False))
#   .withColumn("order_filled", lit(False)) # potentially use to filter the read on the merge
  .withColumn("modified_datetime", current_timestamp())
  .withColumn("created_datetime", current_timestamp())
  .select("customer_id", "order_id", "store_id", "attr.product_id", "attr.qty", "attr.total", "filled", "modified_datetime", "created_datetime")
  .writeStream
  .option('checkpointLocation', bronze_ckpts+"/ops_order_line_items")
  .toTable("ops_order_line_items")
)

# COMMAND ----------

display(spark.read.table("ops_order_line_items").limit(10))

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
not_strm = (
  spark.readStream
  .option('ignoreChanges', True)
  .table("ops_orders_main")
  .filter(col("filled") == True)
  .filter(col('item_cnt') > 0)
  .distinct()
  .withColumn("created_datetime", current_timestamp())
  .withColumn("message", lit("Hello! All the items in your order have been picked."))
  .writeStream
  .option('checkpointLocation', bronze_ckpts+"/ops_customer_notifications")
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

status_strm = (
  spark.readStream
  .option('ignoreChanges', True)
  .table('ops_order_line_items')
  .filter(col('filled') == True)
  .distinct()
  .select("order_id")
  .groupBy("order_id")
  .count()
  .writeStream
  .option("checkpointLocation", bronze_ckpts+"/ops_orders_main_line_item_merge")
  .outputMode("complete")
  .foreachBatch(merge_order_status)
  .start()
     )

# COMMAND ----------

display(spark.read.table('ops_orders_main').limit(100))

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

# COMMAND ----------

# Write to both the pickup and delivery completed orders table 
comp_strm = (
  spark.readStream
  .option('ignoreChanges', True)
  .table('ops_orders_main')
  .filter(col('filled') == True)
  .filter(col('item_cnt') > 0)
  .withColumn("transaction_completed", lit(False))
  .writeStream
  .option("checkpointLocation", bronze_ckpts+"/ops_completed_orders")
  .foreachBatch(write_completed_order)
  .start()
)

# COMMAND ----------

# wait for both tables to be created. Demux causes a delay in table creation for the secondary table(s) 
while spark.catalog.tableExists('ops_pickup_completed_orders') == False and spark.catalog.tableExists('ops_delivery_completed_orders') == False:
  time.sleep(3)

# COMMAND ----------


## ops_customer_notifications
# - writes to customer notification table ("append only") when the order has been picked up by the customer
# - receipt is sent to customer
not_strm2 = (
  spark.readStream
  .option('ignoreChanges', True)
  .table("ops_pickup_completed_orders")
  .filter(col("transaction_completed") == True)
  .distinct()
  .withColumn("created_datetime", current_timestamp())
  .withColumn("message", lit("Hello! Your pickup order has been completed. Please review your receipt."))
  .writeStream
  .option("mergeSchema", "true")
  .option('checkpointLocation', bronze_ckpts+"/ops_customer_completed_pickup_transactions_notifications")
  .toTable("ops_customer_notifications")
)

# COMMAND ----------

# there can be metadata conflicts if this stream and the previous start too close to each other causing a failure. 
# we will do a simple wait to ensure no issues 
time.sleep(15)

## ops_customer_notifications
# - writes a notification when the order has been delivered to the customer 
# - receipt is sent to customer
not_strm3 = (
  spark.readStream
  .option('ignoreChanges', True)
  .table("ops_delivery_completed_orders")
  .filter(col("transaction_completed") == True)
  .distinct()
  .withColumn("created_datetime", current_timestamp())
  .withColumn("message", lit("Hello! Your delivery order has been completed. Please review your receipt."))
  .writeStream
  .option("mergeSchema", "true")
  .option('checkpointLocation', bronze_ckpts+"/ops_customer_completed_delivery_transactions_notifications")
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

dbutils.fs.mkdirs("{}/picking_updates/".format(raw_files))
def fulfill_order(item_pdf):
  end_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=60) 
  sleep_time = .5
  i = 0
  oids = list(items_pdf.order_id.unique())

  while datetime.datetime.utcnow() < end_time:
    d = items_pdf[items_pdf['order_id'] == oids[i]].to_dict(orient='records')
    filename = "/dbfs{}/picking_updates/picking_updates_{}.json".format(raw_files, str(uuid.uuid4()))
    with open(filename, "w") as f:
      json.dump(d, f)

    time.sleep(sleep_time)
    i+=1

# COMMAND ----------

# execute thread
items_pdf = spark.read.table("ops_order_line_items").select('order_id', 'product_id', lit(True).alias('Filled')).limit(50000).toPandas()
x = threading.Thread(target=fulfill_order, args=(items_pdf,))
x.start()


# COMMAND ----------

picking_updates_df = read_ingest_json_data('picking_updates')
write_ingest_json_data(picking_updates_df, 'ops_order_line_items_picking')


## foreach batch merge function 
# used to update an order status when one or many items are filled in an order 
def merge_order_status(microBatchDF, batchId):
  microBatchDF = microBatchDF.withColumn("batchId", lit(batchId)).distinct()
  mytable = DeltaTable.forName(spark, 'ops_order_line_items')
  
  (
    mytable.alias('target')
    .merge(microBatchDF.alias('source'), 
          "target.order_id = source.order_id and target.product_id = source.product_id"
          )
    .whenMatchedUpdate(set = 
          {
            "filled": "source.filled",
            "modified_datetime": current_timestamp()
          }
      ).execute()
  )

# COMMAND ----------

## Updates ops_order_main with the number of line items that have been filled
# - maintains order state/status
# - when implemented `order_filled` can be used to filter the line items and reduce the size 

(picking_updates_df
  .writeStream
  .option("checkpointLocation", bronze_ckpts+"/ops_orders_main_line_item_picking")
  .foreachBatch(merge_order_status)
  .start()
     )

# COMMAND ----------

# DBTITLE 1,WIP - need a way to update that the order has been delivered to or picked up by customer
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

# MAGIC %sql
# MAGIC SELECT * FROM ops_order_line_items where filled = True

# COMMAND ----------

# DBTITLE 1,Let's check out the orders being filled
display(spark.read.table("ops_orders_main").filter(col("filled") == True))

# COMMAND ----------

# DBTITLE 1,Customers being notified of their order status
display(spark.read.table("ops_customer_notifications").select("customer_id", "datetime", "order_id", "message"))

# COMMAND ----------


