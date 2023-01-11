# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Data  
# MAGIC 
# MAGIC In this demo we will generate fake data using Faker to simulate point of sale data, inventory, customer, and vendor data. 

# COMMAND ----------

dbutils.widgets.dropdown('ResetSchema',defaultValue='Yes', choices=['Yes', 'No'])
dbutils.widgets.text("SchemaName", "")

# COMMAND ----------

num_customers = 100
num_stores = 20 
user_name = spark.sql("SELECT current_user()").collect()[0][0]

raw_files = "/dbfs/Users/{}/retail_demo/raw/data".format(user_name)
spark_raw_files = raw_files.replace("/dbfs", "")
raw_ckpts = "/Users/{}/retail_demo/raw/raw_ckpts".format(user_name)
raw_schemas = "/Users/{}/retail_demo/raw/raw_schemas".format(user_name)
bronze_ckpts = "/Users/{}/retail_demo/bronze/bronze_ckpts".format(user_name)

# COMMAND ----------

# if we rerun this entire notebook then we need to reset all of our metadata 
# go to "Continue Generating Data" if you want to pick up where you left off 
if dbutils.widgets.get("ResetSchema") == "Yes":
  print("Dropping Database and Deleting Data")
  dbutils.fs.rm("/Users/{}/retail_demo".format(user_name), True)
  spark.sql("DROP SCHEMA IF EXISTS {} CASCADE".format(dbutils.widgets.get("SchemaName")))

# COMMAND ----------

dbutils.fs.mkdirs(raw_files.replace('/dbfs', ''))
dbutils.fs.mkdirs(raw_ckpts)
dbutils.fs.mkdirs(raw_schemas)
dbutils.fs.mkdirs(bronze_ckpts)

# COMMAND ----------

import subprocess 

def pip_install(name):
    subprocess.call(['pip', 'install', name])
    

pip_install('Faker')

# COMMAND ----------

import uuid
import json
import time 
import random
import datetime 
import pandas as pd
import threading
from lib import * # python imports 

# COMMAND ----------

url = 'https://github.com/rchynoweth/RetailDemo/blob/7078539fc4354692723796833b5a5b7846623af2/src/products.csv'
pdf = pd.read_csv(url)
pdf['product_id'] = pdf['ImgId'][:]
del pdf['ImgId']


customer = customer.Customer()
customer_address = customer_address.CustomerAddress()
store = store.Store()
store_address = store_address.StoreAddress()
products = products.Products(pdf)
order = order.Order()
vendor = vendor.Vendor()
inventory = inventory.Inventory()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Datasets

# COMMAND ----------

def generate_base_data(dataset_obj, dataset_name, n_range):
  dbutils.fs.mkdirs('{}/{}'.format(spark_raw_files, dataset_name))
  
  for i in n_range:
    d = {}
    if dataset_name in ['store', 'customer']:
      d = dataset_obj.create()
    else :
      d = dataset_obj.create(i)
      

    with open("{}/{}/{}_{}.json".format(raw_files, dataset_name, dataset_name, str(uuid.uuid4())), "w") as f:
      json.dump(d, f)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer and Order Data

# COMMAND ----------

# DBTITLE 1,Base Data
# customers
generate_base_data(customer, 'customer', range(0,num_customers))
# stores
generate_base_data(store, 'store', range(0, num_stores))
# customer_address
generate_base_data(customer_address, 'customer_address', customer.customer_ids)
# store_address
generate_base_data(store_address, 'store_address', store.store_ids)

# COMMAND ----------

customer.customer_ids = [cid.customer_id for cid in spark.read.json('{}/customer/customer_*.json'.format(spark_raw_files)).select("customer_id").distinct().collect()]
store.store_ids = [sid.store_id for sid in spark.read.json('{}/store/store_*.json'.format(spark_raw_files)).select("store_id").distinct().collect()]

# COMMAND ----------

# DBTITLE 1,Products
dbutils.fs.mkdirs('{}/products'.format(spark_raw_files))

d = products.get_products_dict()

with open("{}/product/product_{}.json".format(raw_files, str(uuid.uuid4())), "w") as f:
  json.dump(d, f)

display(spark.read.json('{}/product/product_*.json'.format(spark_raw_files)))

# COMMAND ----------

# DBTITLE 1,Orders
dbutils.fs.mkdirs('{}/order'.format(spark_raw_files))
dbutils.fs.mkdirs('{}/order_actions'.format(spark_raw_files))
order_range = range(10,100)

for cid in customer.customer_ids:
  # randomly determine the number of orders to give them and randomly select a store 
  for i in order_range: 
    sid = store.select_random_store()
    d, a = order.create(cid, sid, products)
    filename = "{}/order/order_{}.json".format(raw_files, str(uuid.uuid4()))
    with open(filename, "w") as f:
      json.dump(d, f)

    filename = "{}/order_actions/order_actions_{}.json".format(raw_files, str(uuid.uuid4()))
    with open(filename, "w") as f:
      json.dump(a, f)
      
display(spark.read.json('{}/order/order_*.json'.format(spark_raw_files)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inventory and Vendor Data 

# COMMAND ----------

# MAGIC %md
# MAGIC **Inventory and Vendor Data** 
# MAGIC 
# MAGIC The *inventory* data is aggregated at a day level and is grouped by `store_id` and `product_id`. We will need to generate data for the following columns: 
# MAGIC - `total_sales_units` - this will be obtained using order data
# MAGIC - `on_hand_inventory_units` 
# MAGIC - `replensihment_units`
# MAGIC - `inventory_pipeline`
# MAGIC - `units_in_transit`
# MAGIC - `units_in_dc`
# MAGIC - `units_on_order`
# MAGIC - `units_under_promotion`
# MAGIC - `shelf_capacity`
# MAGIC 
# MAGIC How do we create the data? - we need to simulate the people in the store that are are taking inventory and inputting into the system. 
# MAGIC 
# MAGIC For each store and each product generate the columns above. Please note that while inventory is aggregated by day, it is not a daily measurement as it is unrealistic to expect individuals to take inventory that frequently. 
# MAGIC 
# MAGIC 
# MAGIC The *vendor* data is also required and focuses on the specific lead times for each product. In this case we want to create the columns below which describe the amount of time required to complete the specific time to complete the task. 
# MAGIC - `lead_time_in_transit`: the number of days required to receive the items at a store once it leaves the distribution center 
# MAGIC - `lead_time_in_dc`: the number of days required to get items to a store once it arrives at a distribution center
# MAGIC - `lead_time_on_order`: the number of days required to get items to a store after ordering

# COMMAND ----------

# DBTITLE 1,Inventory
dbutils.fs.mkdirs('{}/inventory'.format(spark_raw_files))

products_df = products.get_products()

# for each product in each store generate the inventory information 
for i in range(0, len(products_df)):
  pid = products_df['product_id'].iloc[i]
  d = inventory.create(pid=pid, stores=store.store_ids)

  filename = "{}/inventory/inventory_{}.json".format(raw_files, str(uuid.uuid4()))
  with open(filename, "w") as f:
    json.dump(d, f)

df = (spark.read.json('{}/inventory/inventory_*.json'.format(spark_raw_files)))
display(df)

# COMMAND ----------

# DBTITLE 1,Vendor
dbutils.fs.mkdirs('{}/vendor'.format(spark_raw_files))
num_stores = 5

products_df = products.get_products()

# for each product in each store generate the vendor information 
for i in range(0, len(products_df)):
  pid = products_df['product_id'].iloc[i]
  vid = int(products_df['vendor_id'].iloc[i])
  d = vendor.create_vendor_data(pid=pid, vid=vid, stores=store.store_ids[0:num_stores])

  filename = "{}/vendor/vendor_{}.json".format(raw_files, str(uuid.uuid4()))
  with open(filename, "w") as f:
    json.dump(d, f)
      
display(spark.read.json('{}/vendor/vendor_*.json'.format(spark_raw_files)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continue Generating Data for Auto Loader purposes 

# COMMAND ----------

# DBTITLE 1,Customer Address - Updates
def customer_address_update(customer_address, customer):
  try:
    end_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=60)
    customer_address_count = 2 
    sleep_time = 2.5

    while datetime.datetime.utcnow() < end_time:
      ## Update an Address
      for i in range(0, customer_address_count):
        customer_id = customer.select_random_customer()
        d = customer_address.update_customer_address(customer_id)
        filename = "{}/customer_address/customer_address_{}.json".format(raw_files, str(uuid.uuid4()))
        with open(filename, "w") as f:
          json.dump(d, f)

      time.sleep(sleep_time)
  except err:
    print(err)

# COMMAND ----------

ca = threading.Thread(target=customer_address_update, args=(customer_address, customer,))
ca.start()

# COMMAND ----------

# DBTITLE 1,Customer Orders - Create
def create_orders(order, customer, store, products):
  end_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=60)
  order_count = 10 
  sleep_time = .5
  
  while datetime.datetime.utcnow() < end_time:
    ## Update an Address
    for i in range(0, order_count):
      customer_id = customer.select_random_customer()
      store_id = store.select_random_store()
      d, a = order.create_order(customer_id, store_id, products)
      filename = "{}/order/order_{}.json".format(raw_files, str(uuid.uuid4()))
      with open(filename, "w") as f:
        json.dump(d, f)
        
      filename = "{}/order_actions/order_actions_{}.json".format(raw_files, str(uuid.uuid4()))
      with open(filename, "w") as f:
        json.dump(a, f)

    time.sleep(sleep_time)

# COMMAND ----------

o = threading.Thread(target=create_orders, args=(order, customer, store, products,))
o.start()

# COMMAND ----------


