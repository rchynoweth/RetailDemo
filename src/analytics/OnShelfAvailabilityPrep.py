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

dbutils.widgets.text("schema_name", "")

# COMMAND ----------

# raw files are published to a user specific directory 
user_name = spark.sql("SELECT current_user()").collect()[0][0]
raw_files = "/Users/{}/retail_demo/raw/data".format(user_name)

schema_name = dbutils.widgets.get("schema_name")

# create and set default database
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE {schema_name}")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

products_df = spark.sql('select product_id, categories as product_category from ops_product')

inventory_df = (spark.read
                .table("ops_inventory")
                .filter(col("_rescued_data").isNull())
                .withColumn('datetime', to_timestamp('datetime'))
                .withColumn('date', to_date(to_timestamp('datetime')))
                .drop('_rescued_data', 'source_file')    
                .join(products_df, 'product_id')
                .withColumnRenamed('product_id', 'sku')
               )
vendor_df = (spark.read
             .table("ops_vendor")
             .filter(col("_rescued_data").isNull())
             .drop('_rescued_data', 'source_file')
            )

# COMMAND ----------

display(inventory_df)

# COMMAND ----------

display(vendor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC The inventory data contains records for products in specific stores when an inventory-related transaction occurs. Since not every product moves on every date, there will be days for which there is no data for certain store and product SKU combinations.
# MAGIC 
# MAGIC Time series analysis techniques used in our framework require a complete set of records for products within a given location. To address the missing entries, we will generate a list of all dates for which we expect records. A cross-join with store-SKU combinations will provide the base set of records for which we expect data.
# MAGIC 
# MAGIC In the real world, not all products are intended to be sold at each location at all times. In an analysis of non-simulated data, we may require additional information to determine the complete set of dates for a given store-SKU combination for which we should have data:

# COMMAND ----------

# DBTITLE 1,Get the missing dates
start_date, end_date = (inventory_df
  .groupBy()
    .agg(
      min('date').alias('start_date'),
      max('date').alias('end_date')  
        )
  .collect()[0]
  )
 
# generate contiguous set of dates within start and end range
dates = (
  spark
    .range( (end_date - start_date).days + 1 )  # days in range
    .withColumn('id', expr('cast(id as integer)')) # range value from long (bigint) to integer
    .withColumn('date', lit(start_date) + col('id'))  # add range value to start date to generate contiguous date range
    .select('date')
  )
 
# display dates
display(dates.orderBy('date'))

# COMMAND ----------

# DBTITLE 1,Assemble Complete Set of Stores-SKUs
# extract unique store-sku combinations in inventory records
store_skus = (inventory_df
    .select('store_id','sku','product_category')
    .groupBy('store_id','sku')
      .agg(last('product_category').alias('product_category')) # just a hack to get last category assigned to each store-sku combination
  )
 
display(store_skus)

# COMMAND ----------

# DBTITLE 1,Generate Inventory Records
# generate one record for each store-sku for each date in range
inventory_with_gaps = (
  dates
    .crossJoin(store_skus)
    .join(
      inventory_df.drop('product_category'), 
      on=['date','store_id','sku'], 
      how='leftouter'
      )
  )
 
# display inventory records
display(inventory_with_gaps)

# COMMAND ----------

# MAGIC %md
# MAGIC We now have one record for each date-store-SKU combination in our dataset. However, on those dates for which there were no inventory changes, we are currently missing information about the inventory status of those stores and SKUs. To address this, we will employ a combination of forward filling, i.e. applying the last valid record to subsequent records until a new value is encountered, and defaults. For the forward fill, we will make use of the last() function, providing a value of True for the ignorenulls argument which will force it to retrieve the last non-null value in a sequence:

# COMMAND ----------

# DBTITLE 1,Fill missing values 
# copy dataframe to enable manipulations in loop
inventory_cleansed = inventory_with_gaps
 
# apply forward fill to appropriate columns
for c in ['shelf_capacity', 'on_hand_inventory_units']:
  inventory_cleansed = (
    inventory_cleansed
      .withColumn(
          c, 
          expr('LAST({0}, True) OVER(PARTITION BY store_id, sku ORDER BY date)'.format(c)) # get last non-null prior value (aka forward-fill)
           )
        )
  
# apply default value of 0 to appropriate columns
inventory_cleansed = (
  inventory_cleansed
    .fillna(
      0, 
      [ 'total_sales_units',
        'units_under_promotion',
        'units_in_transit',
        'units_in_dc',
        'units_on_order',
        'replenishment_units',
        'inventory_pipeline'
        ]
      )
  )
 
# display data with imputed values
display(inventory_cleansed)

# COMMAND ----------

# DBTITLE 1,Calculate Inventory Flags
# derive inventory flags
inventory_final = (
  inventory_cleansed
    .withColumn('promotion_flag', expr('CASE WHEN units_under_promotion > 0 THEN 1 ELSE 0 END'))
    .withColumn('replenishment_flag', expr('CASE WHEN replenishment_units > 0 THEN 1 ELSE 0 END'))
    )
 
display(inventory_final)


# COMMAND ----------

(
  inventory_final.withColumn("processing_datetime", current_timestamp())
  .write
  .saveAsTable("osa_inventory")
)

# COMMAND ----------


