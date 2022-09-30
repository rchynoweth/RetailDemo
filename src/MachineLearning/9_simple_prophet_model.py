# Databricks notebook source
# MAGIC %md
# MAGIC # Train Machine Learning Model 

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd

# COMMAND ----------

from prophet import Prophet

# COMMAND ----------

dbutils.widgets.text("schema_name", "")

# COMMAND ----------

schema_name = dbutils.widgets.get("schema_name")
spark.sql(f"USE {schema_name}")

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

df = (spark.read
      .table("ops_order")
      .filter(col("_rescued_data").isNull())
      .withColumn("item_cnt", size(from_json(col("basket"), basket_schema))) 
      .select("datetime", "store_id", 'order_source', 'order_type', "item_cnt")
      .withColumn("ds", to_date("datetime"))
    )

display(df)

# COMMAND ----------

pdf = df.select("ds", "item_cnt").groupBy("ds").agg(sum("item_cnt").alias("y")).toPandas()
pdf.head()

# COMMAND ----------

model = Prophet()
 
# fit the model to historical data
model.fit(pdf)

# COMMAND ----------

future = model.make_future_dataframe(periods=365)
forecast = model.predict(future)
# forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail()
forecast.tail()

# COMMAND ----------

# Python
fig1 = model.plot(forecast)


# COMMAND ----------

fig2 = model.plot_components(forecast)

# COMMAND ----------


