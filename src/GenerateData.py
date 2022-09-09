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

num_customers = 100
num_stores = 20 
user_name = spark.sql("SELECT current_user()").collect()[0][0]

raw_files = "/dbfs/Users/{}/dynamic_dlt/raw".format(user_name)

# COMMAND ----------

# if we rerun this entire notebook then we need to reset all of our metadata 
# go to "Continue Generating Data" if you want to pick up where you left off 
dbutils.fs.rm(raw_files.replace('/dbfs', ''), True)
dbutils.fs.rm(raw_files.replace('/dbfs', '')+"_ckpts", True)
dbutils.fs.rm(raw_files.replace('/dbfs', '')+"_schemas", True)
dbutils.fs.rm("/Users/{}/dynamic_dlt/ops_ckpts".format(user_name), True)

# COMMAND ----------

dbutils.fs.ls("/Users/{}/dynamic_dlt/".format(user_name))

# COMMAND ----------

dbutils.fs.mkdirs(raw_files.replace('/dbfs', ''))
dbutils.fs.mkdirs(raw_files.replace('/dbfs', '')+"_ckpts")
dbutils.fs.mkdirs(raw_files.replace('/dbfs', '')+"_schemas")
dbutils.fs.mkdirs("/Users/{}/dynamic_dlt/ops_ckpts".format(user_name))

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
from faker import Faker

from lib.customer import Customer 
from lib.customer_address import CustomerAddress
from lib.store import Store
from lib.store_address import StoreAddress 
from lib.products import Products
from lib.order import Order 
from lib.data_domain import DataDomain 

fake = Faker()

# COMMAND ----------

url = 'https://github.com/rchynoweth/DemoContent/raw/main/delta_demos/DLT/dynamic_dlt/train.csv'
pdf = pd.read_csv(url)
pdf['product_id'] = pdf['ImgId'][:]
del pdf['ImgId']


customer = Customer()
customer_address = CustomerAddress()
store = Store()
store_address = StoreAddress()
products = Products(pdf)
order = Order()
data_domain = DataDomain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Dataset 

# COMMAND ----------

# DBTITLE 1,Customers
dbutils.fs.mkdirs('Users/{}/dynamic_dlt/raw/customer'.format(user_name))
for i in range(0, num_customers):
  d = customer.create_customer()
  data_domain.add_customer(d.get('customer_id'))
  
  with open("{}/customer/customer_{}.json".format(raw_files, str(uuid.uuid4())), "w") as f:
    json.dump(d, f)
    
display(spark.read.json('Users/{}/dynamic_dlt/raw/customer/customer_*.json'.format(user_name)))

# COMMAND ----------

# DBTITLE 1,Stores
dbutils.fs.mkdirs('Users/{}/dynamic_dlt/raw/store'.format(user_name))
for i in range(0, num_stores):
  d = store.create_store()
  data_domain.add_store(d.get('store_id'))

  
  with open("{}/store/store_{}.json".format(raw_files, str(uuid.uuid4())), "w") as f:
    json.dump(d, f)


display(spark.read.json('Users/{}/dynamic_dlt/raw/store/store_*.json'.format(user_name)))

# COMMAND ----------

# DBTITLE 1,Customer Addresses
dbutils.fs.mkdirs('Users/{}/dynamic_dlt/raw/customer_address'.format(user_name))
for cid in data_domain.customer_ids:
  d = customer_address.create_customer_address(cid)
  
  with open("{}/customer_address/customer_address_{}.json".format(raw_files, str(uuid.uuid4())), "w") as f:
    json.dump(d, f)
    
display(spark.read.json('Users/{}/dynamic_dlt/raw/customer_address/customer_address_*.json'.format(user_name)))

# COMMAND ----------

# DBTITLE 1,Store Addresses
dbutils.fs.mkdirs('Users/{}/dynamic_dlt/raw/store_address'.format(user_name))
for sid in data_domain.store_ids:
  d = store_address.create_store_address(sid)
  
  with open("{}/store_address/store_address_{}.json".format(raw_files, str(uuid.uuid4())), "w") as f:
    json.dump(d, f)
    

display(spark.read.json('Users/{}/dynamic_dlt/raw/store_address/store_address_*.json'.format(user_name)))

# COMMAND ----------

# DBTITLE 1,Products
dbutils.fs.mkdirs('Users/{}/dynamic_dlt/raw/product'.format(user_name))

d = products.get_products_dict()
with open("{}/product/product_{}.json".format(raw_files, str(uuid.uuid4())), "w") as f:
  json.dump(d, f)

display(spark.read.json('Users/{}/dynamic_dlt/raw/product/product_*.json'.format(user_name)))

# COMMAND ----------

# DBTITLE 1,Orders
# for each customer 
dbutils.fs.mkdirs('Users/{}/dynamic_dlt/raw/order'.format(user_name))
order_range = range(10,100)

for cid in data_domain.customer_ids:
  # randomly determine the number of orders to give them and randomly select a store 
  for i in order_range: 
    sid = data_domain.select_random_store()
    d = order.create_order(cid, sid, products)
    with open("{}/order/order_{}.json".format(raw_files, str(uuid.uuid4())), "w") as f:
      json.dump(d, f)
      
display(spark.read.json('Users/{}/dynamic_dlt/raw/order/order_*.json'.format(user_name)))

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

# DBTITLE 1,Order Creation
def create_orders(order, data_domain, products):
  end_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=60)
  order_count = 10 # at sleep = 5 and cnt = 10 this produces an average of 2 files a second 
  sleep_time = 2.5
  
  while datetime.datetime.utcnow() < end_time:
    ## Update an Address
    for i in range(0, order_count):
      customer_id = data_domain.select_random_customer()
      store_id = data_domain.select_random_store()
      d = order.create_order(customer_id, store_id, products)
      filename = "{}/order/order_{}.json".format(raw_files, str(uuid.uuid4()))
      with open(filename, "w") as f:
        json.dump(d, f)

    time.sleep(sleep_time)

# COMMAND ----------

# execute thread
o = threading.Thread(target=create_orders, args=(order, data_domain, products,))
o.start()


# COMMAND ----------


