# Databricks notebook source
# MAGIC %md
# MAGIC # Train Machine Learning Model 
# MAGIC 
# MAGIC In this notebook we will create a process that trains a machine learning model for each store in our domain. We will be doing simple forecasting using Facebook's prophet library, which is a single threaded library for time series data. In our case we have many stores and we want to be able to train as many models in a distributed fashion as possible. 
# MAGIC 
# MAGIC 
# MAGIC Please refer to the Databricks [Fine Grained Time Series Forecasting](https://www.databricks.com/blog/2021/04/06/fine-grained-time-series-forecasting-at-scale-with-facebook-prophet-and-apache-spark-updated-for-spark-3.html) blog for more information and this [notebook](https://www.databricks.com/notebooks/recitibikenycdraft/time-series.html) for anothe example. 
# MAGIC 
# MAGIC [MLflow Prophet Doc](https://www.mlflow.org/docs/latest/python_api/mlflow.prophet.html)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from databricks.feature_store import feature_table, FeatureStoreClient
from prophet import Prophet
import pandas as pd
import shutil 
import mlflow
import time
import random
import os 
from PIL import Image

# COMMAND ----------

dbutils.widgets.text("schema_name", "")

# COMMAND ----------

schema_name = dbutils.widgets.get("schema_name")
spark.sql(f"USE {schema_name}")

# COMMAND ----------

user_name = spark.sql("SELECT current_user()").collect()[0][0]
ml_path = "/dbfs/Users/{}/retail_demo/ml_models/store_item_sum_forecasting_daily".format(user_name)
c = mlflow.tracking.MlflowClient()

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

result_schema =StructType([
  StructField('ds',DateType()),
  StructField('store_id',StringType()),
  StructField('y',FloatType()),
  StructField('yhat',FloatType()),
  StructField('yhat_upper',FloatType()),
  StructField('yhat_lower',FloatType())
  ])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Write to Feature Store
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="500" />
# MAGIC 
# MAGIC 
# MAGIC Databricks Feature Store is a centralized repository of features. It enables feature sharing and discovery across your organization and also ensures that the same feature computation code is used for model training and inference.  
# MAGIC 
# MAGIC 
# MAGIC ### Why Feature Store?
# MAGIC 
# MAGIC Raw data needs to be processed and transformed before it can be used in machine learning. This process is called “feature engineering” and includes transformations such as aggregating data (for example, the number of purchases by a user in a given time window) and more complex calculations that may themselves be the result of machine learning algorithms such as word embeddings. 
# MAGIC 
# MAGIC Converting raw data into features for model training is time-consuming. Creating and maintaining feature definition pipelines requires significant effort. Teams often want to explore and leverage features created by other data scientists in the organization.  
# MAGIC 
# MAGIC Another challenge is maintaining consistency between training and serving. A feature pipeline might be created by a data scientist and reimplemented by an engineer for model serving in production. This is slow and may affect the performance of the model once deployed if data drifts. 
# MAGIC 
# MAGIC The Databricks feature store allows users to create, explore, and re-use existing features. Feature store datasets can easily be published for real-time inference. 
# MAGIC 
# MAGIC Feature tables are stored as Delta Tables, and if models are deployed with MLflow then models can automatically retrieve features from the Feature Store making retraining and scoring simple. Feature store will bring traceability and governance in our deployment, knowing which model is dependent of which set of features.  
# MAGIC 
# MAGIC #### Concepts
# MAGIC 
# MAGIC **Feature Table** - features are stored in delta tables and contain additional metadata information. Feature tables must have a primary key and are computed and updated using a common computation function. The table metadata tracks the data lineage of the table and all notebooks and jobs that are associated with the table.  
# MAGIC 
# MAGIC **Online Store**  - an online store is a low-latency database used for real-time model inference. The following stores are supported:  
# MAGIC - Amazon Aurora (MySQL compatible)  
# MAGIC - Amazon RDS MySQL  
# MAGIC 
# MAGIC **Offline Store** - offline feature store is used during the development process for feature discovery and training. It is also used for batch inference and contains tables that are stored as Delta Tables. This is one of the more common usages since it is ideal for batch predictions.   
# MAGIC 
# MAGIC **Streaming** - In addition to batch processing. You can write feature values to a feature table from a streaming source and the computation code can utilize structured streaming to transform raw data streams into features.  
# MAGIC 
# MAGIC **Training Set** - A training set consists of a list of features and a DataFrame containing raw training data, labels, and primary keys by which to look up features. You create the training set by specifying features to extract from the store and provide this set during the model training process.  
# MAGIC 
# MAGIC **Model packaging** - a machine learning model trained using features from Databricks feature store retains references to these features. At inference time the model can optionally retrieve feature values from the store. The caller only needs to provide the primary key of the features used in the model and the model will retrieve all features required. This is done with `FeatureStoreClient.log_model()`.   
# MAGIC 
# MAGIC 
# MAGIC <br></br>
# MAGIC ### Batch Deployment   
# MAGIC For batch use cases, the model automatically retrieves the features it needs from Feature Store.
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_images/feature-store-flow-gcp.png"/>
# MAGIC 
# MAGIC <br></br>
# MAGIC <br></br>
# MAGIC ### Real-time Deployment
# MAGIC At inference time, the model reads pre-computed features from the online feature store and joins them with the data provided in the client request to the model serving endpoint.  
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_images/feature-store-flow-with-online-store.png" />

# COMMAND ----------

df = spark.read.table("ops_order")

# COMMAND ----------

def transform_data(df):
  return (df
          .filter(col("_rescued_data").isNull())
          .withColumn("item_cnt", size(from_json(col("basket"), basket_schema))) 
          .withColumn("ds", to_date("datetime"))
          .select("store_id", "ds", "item_cnt")
          .groupBy("store_id", "ds")
          .agg(sum("item_cnt").alias("y"))
          .withColumn("id", monotonically_increasing_id())
          )

# COMMAND ----------

history_df = transform_data(df)
display(history_df)

# COMMAND ----------

# DBTITLE 1,Create Feature Store
fs = FeatureStoreClient()

try:
  #drop table if exists
  fs.drop_table(f'{schema_name}.store_item_forecasting_input')
except:
  pass

forecasting_feature_table = fs.create_table(
  name=f'{schema_name}.store_item_forecasting_input',
  primary_keys='id',
  schema=history_df.schema,
  description='These features are used for daily store item forecasting. Aggregations to the daily and store level were applied.'
)

fs.write_table(df=history_df, name=f'{schema_name}.store_item_forecasting_input', mode='overwrite')

# COMMAND ----------

def forecast_store(history_pd):
  with mlflow.start_run() as run: 
    # Start a timer to get overall elapsed time for this function
    overall_start_time = time.time()

    # Log a Tag for the run
    mlflow.set_tag("Owner", "ryan.chynoweth@databricks.com")
    
    
    store_id = history_pd.iloc[0][0]
    mlflow.set_tag('store_id', store_id)
    print(f"Store ID: {store_id}")
    model = Prophet()
    model.fit(history_pd)

    future_pd = model.make_future_dataframe(periods=365)
    forecast_pd = model.predict(future_pd)

    ####### Save Information #######
    model_path = '/dbfs/{}/{}'.format(ml_path, store_id)
    shutil.rmtree(model_path, ignore_errors=True)
    mlflow.prophet.save_model( model, model_path)
#     mlflow.log_artifact(model_path)
    mlflow.prophet.log_model(model, "prophet-model")

    mlflow.log_metric("Training Data Rows", len(history_pd))
    
    ####### Log Images #######
    fig1 = model.plot(forecast_pd)
    fig2 = model.plot_components(forecast_pd)
    mlflow.log_figure(fig1, f"{store_id}_plot.png")
    mlflow.log_figure(fig2, f"{store_id}_plot_components.png")


    ####### Assemble Results #######
    f_pd = forecast_pd[ ['ds','yhat', 'yhat_upper', 'yhat_lower'] ].set_index('ds')

    # get relevant fields from history
    h_pd = history_pd[['ds','store_id','y']].set_index('ds')
    

    # join history and forecast
    results_pd = f_pd.join( h_pd, how='left' )
    results_pd.reset_index(level=0, inplace=True)
    overall_end_time = time.time()
    overall_elapsed_time = overall_end_time - overall_start_time
    mlflow.log_metric("Overall Elapsed Time", overall_elapsed_time)
    mlflow.log_metric("Evaluation Metric", random.random())
  
  return results_pd

# COMMAND ----------

results = (
  history_df
      .groupBy('store_id')
        .applyInPandas(forecast_store, schema=result_schema)
      .withColumn('training_datetime', current_timestamp())
  )

# COMMAND ----------

results.write.mode("append").saveAsTable("item_forecast_by_store")

# COMMAND ----------

display(spark.sql("SELECT * FROM item_forecast_by_store"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Programatically Select Model by Store ID

# COMMAND ----------

df_client = spark.read.format("mlflow-experiment").load("9bd608e04d164d95a2acdb7767bb42cb")
df_client.createOrReplaceTempView("vw_experiment_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC   select tags.store_id as store_id
# MAGIC   , *
# MAGIC   , concat(artifact_uri,'/prophet-model') as model_uri 
# MAGIC 
# MAGIC   from vw_experiment_data 
# MAGIC 
# MAGIC   where status='FINISHED' and tags.store_id = 'e9941c94-79c6-4569-9529-1a1f1b8f747b'
# MAGIC 
# MAGIC   order by start_time desc

# COMMAND ----------

store_id = 'e9941c94-79c6-4569-9529-1a1f1b8f747b'
  
# select data 
model_df = spark.sql("""
  select tags.store_id as store_id
  , *
  , concat(artifact_uri,'/prophet-model') as model_uri 

  from vw_experiment_data 

  where status='FINISHED' and tags.store_id = '{}'

  order by start_time desc
  """.format(store_id)
)



# COMMAND ----------

display(model_df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# create dowload dir for images
if os.path.exists("/tmp/"):
  print("Deleting Download Directory...")
  shutil.rmtree("/tmp")
  os.makedirs("/tmp", exist_ok=False)

selected_model_id = model_df.select("run_id").first()[0]
print("Model ID = '{}'".format(selected_model_id))

# COMMAND ----------

# create dowload dir for images
if os.path.exists("/tmp/"):
  print("Deleting Download Directory...")
  shutil.rmtree("/tmp")
  os.makedirs("/tmp", exist_ok=False)

# COMMAND ----------

c.download_artifacts(selected_model_id, f"{store_id}_plot_components.png", "/tmp/")

# COMMAND ----------

c.download_artifacts(selected_model_id, f"{store_id}_plot.png", "/tmp/")

# COMMAND ----------

fig1 = Image.open(f"/tmp/{store_id}_plot.png")
fig1.show()

# COMMAND ----------

fig1.close()

# COMMAND ----------

fig2 = Image.open(f"/tmp/{store_id}_plot_components.png")
fig2.show()

# COMMAND ----------

fig2.close()
