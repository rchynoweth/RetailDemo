# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Data  
# MAGIC 
# MAGIC In this demo we will generate fake data using Faker to simulate point of sale data. Please note the tables with brief explainations.  
# MAGIC - customer 
# MAGIC - customer_address 
# MAGIC - product 
# MAGIC - order 
# MAGIC - store
# MAGIC - store_address

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
ops_ckpts = "/Users/{}/retail_demo/ops/ops_ckpts".format(user_name)

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
dbutils.fs.mkdirs(ops_ckpts)

# COMMAND ----------

dbutils.fs.ls("/Users/{}/retail_demo/".format(user_name)) # should have 2 dirs 

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

from lib.customer import Customer 
from lib.customer_address import CustomerAddress
from lib.store import Store
from lib.store_address import StoreAddress 
from lib.products import Products
from lib.order import Order 
from lib.data_domain import DataDomain 
from lib.vendor import Vendor

# COMMAND ----------

url = 'https://raw.githubusercontent.com/rchynoweth/RetailDemo/main/src/train.csv'
pdf = pd.read_csv(url)
pdf['product_id'] = pdf['ImgId'][:]
del pdf['ImgId']


customer = Customer()
customer_address = CustomerAddress()
store = Store()
store_address = StoreAddress()
products = Products(pdf)
order = Order()
vendor = Vendor()
data_domain = DataDomain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer and Order Data

# COMMAND ----------

# DBTITLE 1,Customers
dbutils.fs.mkdirs('{}/customer'.format(spark_raw_files))

for i in range(0, num_customers):
  d = customer.create_customer()
  data_domain.add_customer(d.get('customer_id'))
  
  with open("{}/customer/customer_{}.json".format(raw_files, str(uuid.uuid4())), "w") as f:
    json.dump(d, f)
    
display(spark.read.json('{}/customer/customer_*.json'.format(spark_raw_files)))

# COMMAND ----------

# DBTITLE 1,Stores
dbutils.fs.mkdirs('{}/store'.format(spark_raw_files))
for i in range(0, num_stores):
  d = store.create_store()
  data_domain.add_store(d.get('store_id'))

  
  with open("{}/store/store_{}.json".format(raw_files, str(uuid.uuid4())), "w") as f:
    json.dump(d, f)


display(spark.read.json('{}/store/store_*.json'.format(spark_raw_files)))

# COMMAND ----------

# DBTITLE 1,Customer Addresses
dbutils.fs.mkdirs('{}/customer_address'.format(spark_raw_files))
for cid in data_domain.customer_ids:
  d = customer_address.create_customer_address(cid)
  
  with open("{}/customer_address/customer_address_{}.json".format(raw_files, str(uuid.uuid4())), "w") as f:
    json.dump(d, f)
    
display(spark.read.json('{}/customer_address/customer_address_*.json'.format(spark_raw_files)))

# COMMAND ----------

# DBTITLE 1,Store Addresses
dbutils.fs.mkdirs('{}/store_address'.format(spark_raw_files))
for sid in data_domain.store_ids:
  d = store_address.create_store_address(sid)
  
  with open("{}/store_address/store_address_{}.json".format(raw_files, str(uuid.uuid4())), "w") as f:
    json.dump(d, f)
    

display(spark.read.json('{}/store_address/store_address_*.json'.format(spark_raw_files)))

# COMMAND ----------

# DBTITLE 1,Products
dbutils.fs.mkdirs('{}/product'.format(spark_raw_files))

d = products.get_products_dict()

with open("{}/product/product_{}.json".format(raw_files, str(uuid.uuid4())), "w") as f:
  json.dump(d, f)

display(spark.read.json('{}/product/product_*.json'.format(spark_raw_files)))

# COMMAND ----------

# DBTITLE 1,Orders
dbutils.fs.mkdirs('{}/order'.format(spark_raw_files))
dbutils.fs.mkdirs('{}/order_actions'.format(spark_raw_files))
order_range = range(10,100)

for cid in data_domain.customer_ids:
  # randomly determine the number of orders to give them and randomly select a store 
  for i in order_range: 
    sid = data_domain.select_random_store()
    d, a = order.create_order(cid, sid, products)
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

# DBTITLE 1,Inventory - not implemented!
# # for each customer 
# dbutils.fs.mkdirs('Users/{}/retail_demo/raw/inventory'.format(user_name))



# for cid in data_domain.customer_ids:
#   # randomly determine the number of orders to give them and randomly select a store 
#   for i in order_range: 
#     sid = data_domain.select_random_store()
#     d, a = order.create_order(cid, sid, products)
#     filename = "{}/order/order_{}.json".format(raw_files, str(uuid.uuid4()))
#     with open(filename, "w") as f:
#       json.dump(d, f)

#     filename = "{}/order_actions/order_actions_{}.json".format(raw_files, str(uuid.uuid4()))
#     with open(filename, "w") as f:
#       json.dump(a, f)
      
# display(spark.read.json('Users/{}/retail_demo/raw/order/order_*.json'.format(user_name)))

# COMMAND ----------

# DBTITLE 1,Vendor
dbutils.fs.mkdirs('{}/vendor'.format(spark_raw_files))

products_df = products.get_products()

for i in range(0, len(products_df)):
  pid = products_df['product_id'].iloc[i]
  vid = int(products_df['vendor_id'].iloc[i])
  
  for sid in data_domain.store_ids: 
    
    key, d = vendor.create_vendor_data(pid=pid, vid=vid, sid=sid)

    filename = "{}/vendor/vendor_{}.json".format(raw_files, key)
    with open(filename, "w") as f:
      json.dump(d, f)
      
display(spark.read.json('{}/vendor/vendor_*.json'.format(spark_raw_files)))

# COMMAND ----------

d

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continue Generating Data for Auto Loader purposes 

# COMMAND ----------

# DBTITLE 1,Customer Address Updates
def customer_address_update(customer_address, data_domain):
  end_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=60)
  customer_address_count = 2 
  sleep_time = 2.5
  
  while datetime.datetime.utcnow() < end_time:
    ## Update an Address
    for i in range(0, customer_address_count):
      customer_id = data_domain.select_random_customer()
      d = customer_address.update_customer_address(customer_id)
      filename = "{}/customer_address/customer_address_{}.json".format(raw_files, str(uuid.uuid4()))
      with open(filename, "w") as f:
        json.dump(d, f)

    time.sleep(sleep_time)

# COMMAND ----------

# execute thread
ca = threading.Thread(target=customer_address_update, args=(customer_address, data_domain,))
ca.start()


# COMMAND ----------

# DBTITLE 1,Customer Behavior - Order Creation
def create_orders(order, data_domain, products):
  end_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=60)
  order_count = 10 
  sleep_time = 1
  
  while datetime.datetime.utcnow() < end_time:
    ## Update an Address
    for i in range(0, order_count):
      customer_id = data_domain.select_random_customer()
      store_id = data_domain.select_random_store()
      d, a = order.create_order(customer_id, store_id, products)
      filename = "{}/order/order_{}.json".format(raw_files, str(uuid.uuid4()))
      with open(filename, "w") as f:
        json.dump(d, f)
        
      filename = "{}/order_actions/order_actions_{}.json".format(raw_files, str(uuid.uuid4()))
      with open(filename, "w") as f:
        json.dump(a, f)

    time.sleep(sleep_time)

# COMMAND ----------

# execute thread
o = threading.Thread(target=create_orders, args=(order, data_domain, products,))
o.start()


# COMMAND ----------


