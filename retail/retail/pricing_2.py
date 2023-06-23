# Databricks notebook source
# MAGIC %pip install mlflow pymongo

# COMMAND ----------

from pymongo import MongoClient
MONGO_CONN = 'mongodb+srv://<username>:<password>@retail-demo.2wqno.mongodb.net/?retryWrites=true&w=majority'
client = MongoClient(MONGO_CONN)

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, WindowSpec
from pyspark.sql.functions import struct

import mlflow.pyfunc
from tqdm import tqdm
tqdm.pandas()

import warnings
warnings.filterwarnings("ignore")

sales = spark.read.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "processed_clogs_n").\
            load()

# COMMAND ----------

sales = sales.groupby("product_uid").agg(F.sum("total_sales").alias("total_sales"), F.avg("avg_price").alias("avg_price"), F.avg("max_price").alias("max_price"), F.avg("min_price").alias("min_price"),\
                                F.avg("old_avg_price").alias("old_avg_price"), F.sum("old_sales").alias("old_sales") )

model_name = "retail_competitive_pricing_model_1"
apply_model_udf = mlflow.pyfunc.spark_udf(spark, f"models:/{model_name}/staging")
    
# Apply the model to the new data
columns = ['old_sales','total_sales','min_price','max_price','avg_price','old_avg_price']
udf_inputs = struct(*columns)
udf_inputs

# COMMAND ----------

sales = sales.withColumn("pred_price",apply_model_udf(udf_inputs))
sales = sales.withColumn("price_elasticity", F.expr("((old_sales - total_sales)/(old_sales + total_sales))/(((old_avg_price - avg_price)+1)/(old_avg_price + avg_price))"))

# COMMAND ----------

sales.select("product_uid", "pred_price", "price_elasticity").write.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "price").\
            option('spark.mongodb.idFieldList', 'product_uid').\
            mode('append').\
            save()

# COMMAND ----------

