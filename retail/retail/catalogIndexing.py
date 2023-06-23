# Databricks notebook source
# MAGIC %pip install pymongo tqdm

# COMMAND ----------

import pandas as pd
import json
from collections import Counter
from tqdm import tqdm
from pymongo import MongoClient
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
conf = pyspark.SparkConf()


import copy
import numpy as np


tqdm.pandas()

MONGO_CONN = 'mongodb+srv://<username>:<password>@retail-demo.2wqno.mongodb.net/?retryWrites=true&w=majority'

# COMMAND ----------

# from pymongo import MongoClient

# client = MongoClient(MONGO_CONN)
# db = client["search"]
# db['catalog_n'].delete_many({})
# pid = pd.DataFrame.from_records(db['catalog_n'].find({},{"_id": 0, "product_uid": 1}))
# atp = pd.DataFrame.from_records(db['atp_status'].find({},{"_id":0}))
# df = pid.merge(atp, on='product_uid', how="left")
# db['atp_status'].delete_many({})
# db['atp_status'].insert_many(df.to_dict(orient='records'))

# COMMAND ----------

atp = spark.readStream.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "atp_status").\
            option('spark.mongodb.change.stream.publish.full.document.only','true').\
            option('spark.mongodb.aggregation.pipeline',[]).\
            option("forceDeleteTempCheckpointLocation", "true").load()

atp = atp.drop("_id")
atp.writeStream.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "catalog_n").\
            option('spark.mongodb.operationType', "update").\
            option('spark.mongodb.upsertDocument', True).\
            option('spark.mongodb.idFieldList', "product_uid").\
            option("checkpointLocation", "/tmp/retail-atp-4/_checkpoint/").\
            option("forceDeleteTempCheckpointLocation", "true").\
            outputMode("append").\
            start()

# COMMAND ----------



# COMMAND ----------

attrs=spark.readStream.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "attrs").\
            option('spark.mongodb.change.stream.publish.full.document.only','true').\
            option("forceDeleteTempCheckpointLocation", "true").\
            option('spark.mongodb.aggregation.pipeline',[{"$project":{'product_id':0}}]).\
            option("forceDeleteTempCheckpointLocation", "true").load()

attrs = attrs.drop("_id")
attrs.writeStream.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "catalog_n").\
            option('spark.mongodb.idFieldList', 'product_uid').\
            option("checkpointLocation", "/tmp/retail-attrs/_checkpoint/").\
            option("forceDeleteTempCheckpointLocation", "true").\
            outputMode("append").\
            start()

# COMMAND ----------

prd_desc=spark.readStream.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "prd_desc").\
            option('spark.mongodb.change.stream.publish.full.document.only','true').\
            option("forceDeleteTempCheckpointLocation", "true").\
            option('spark.mongodb.aggregation.pipeline',[]).\
            option("forceDeleteTempCheckpointLocation", "true").load()

prd_desc = prd_desc.drop("_id")
prd_desc.writeStream.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "catalog_n").\
            option('spark.mongodb.idFieldList', 'product_uid').\
            option("checkpointLocation", "/tmp/retail-prd-desc/_checkpoint/").\
            option("forceDeleteTempCheckpointLocation", "true").\
            outputMode("append").\
            start()

# COMMAND ----------

prd_score=spark.readStream.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "prd_score").\
            option('spark.mongodb.change.stream.publish.full.document.only','true').\
            option("forceDeleteTempCheckpointLocation", "true").\
            option('spark.mongodb.aggregation.pipeline',[]).\
            option("forceDeleteTempCheckpointLocation", "true").load()

prd_score = prd_score.drop("_id")
prd_score.select("product_uid", "score").writeStream.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "catalog_n").\
            option('spark.mongodb.idFieldList', 'product_uid').\
            option("checkpointLocation", "/tmp/retail-prd-score/_checkpoint/").\
            option("forceDeleteTempCheckpointLocation", "true").\
            outputMode("append").\
            start()

# COMMAND ----------

price = spark.readStream.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "price").\
            option('spark.mongodb.change.stream.publish.full.document.only','true').\
            option("forceDeleteTempCheckpointLocation", "true").\
            option('spark.mongodb.aggregation.pipeline',[{"$project":{"_id":0}}]).\
            option("forceDeleteTempCheckpointLocation", "true").load()

price = price.drop("_id")
price.writeStream.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "catalog_n").\
            option('spark.mongodb.idFieldList', 'product_uid').\
            option("checkpointLocation", "/tmp/retail-price/_checkpoint/").\
            option("forceDeleteTempCheckpointLocation", "true").\
            outputMode("append").\
            start()

# COMMAND ----------

