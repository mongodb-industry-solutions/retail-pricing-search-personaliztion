# Databricks notebook source
import pandas as pd
import json
from collections import Counter
from tqdm import tqdm
import copy
import numpy as np
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from scipy.stats import beta

conf = pyspark.SparkConf()
conf.set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.0")

# COMMAND ----------


MONGO_CONN = 'mongodb+srv://<username>:<password>@retail-demo.2wqno.mongodb.net/?retryWrites=true&w=majority'


spark = SparkSession.builder \
        .master("local") \
        .appName("test1") \
        .config(conf = conf) \
        .getOrCreate()

# COMMAND ----------

# df = spark.read.json("../data/clickmodel-train.jsonl")


pipeline = [
    {
        '$group': {
            '_id': {
                'product_id': '$product_id'
            }, 
            'product_id': {
                '$first': '$product_id'
            },  
            'total_views': {
                '$sum': {
                    '$cond': [
                        {
                            '$in': [
                                '$event_type', [
                                    'view', 'cart', 'purchase'
                                ]
                            ]
                        }, 1, 0
                    ]
                }
            },
            'purchase': {
                '$sum': {
                    '$cond': [
                        {
                            '$in': [
                                '$event_type', [
                                    'purchase'
                                ]
                            ]
                        }, 1, 0
                    ]
                }
            },
            'cart': {
                '$sum': {
                    '$cond': [
                        {
                            '$in': [
                                '$event_type', [
                                    'cart'
                                ]
                            ]
                        }, 1, 0
                    ]
                }
            },
        }
    }, {
        '$project': {
            '_id': 0
        }
    }
]

df = spark.read.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "clogs").\
            option('spark.mongodb.aggregation.pipeline',pipeline).\
            option("forceDeleteTempCheckpointLocation", "true").load()

# COMMAND ----------

@F.udf(T.FloatType())
def beta_fn(pct,a,b):
    return float(100*beta.cdf(pct, a,b))

w = Window().partitionBy()
df = df.withColumn("purchase_alpha", F.avg('purchase').over(w))
df = df.withColumn("cart_alpha", F.avg('cart').over(w))
df = df.withColumn("total_views_mean", F.avg('total_views').over(w))
df = df.withColumn("purchase_beta", F.expr('total_views_mean - purchase_alpha'))
df = df.withColumn("cart_beta", F.expr('total_views_mean - cart_alpha'))
df = df.withColumn("purchase_pct", F.expr('(purchase+purchase_alpha)/(total_views+purchase_alpha+purchase_beta)'))
df = df.withColumn("cart_pct", F.expr('(purchase+cart_alpha)/(total_views+cart_alpha+cart_beta)'))

# COMMAND ----------

df = df.withColumn('purchase_score', beta_fn('purchase_pct', 'purchase_alpha', 'purchase_beta'))
df = df.withColumn('cart_score', beta_fn('cart_pct', 'cart_alpha', 'cart_beta'))

# COMMAND ----------

df = df.withColumn('score', F.expr('purchase_score*0.7 + cart_score*0.3'))

# COMMAND ----------

mapping=spark.read.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "mapping").\
            option('spark.mongodb.aggregation.pipeline',[{"$project":{"_id":0}}]).\
            option("forceDeleteTempCheckpointLocation", "true").load()

# COMMAND ----------

df = df.join(F.broadcast(mapping), on='product_id',how='right')

# COMMAND ----------

df.select("product_uid", "score").write.format('mongodb').\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "prd_score").\
            option('spark.mongodb.operationType', "insert").\
            mode('overwrite').\
            option("forceDeleteTempCheckpointLocation", "true").save()