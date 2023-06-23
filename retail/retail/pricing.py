# Databricks notebook source
# MAGIC %pip install pymongo mlflow

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from statsmodels.formula.api import ols
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import types as T

from datetime import datetime
from pyspark.sql import Window, WindowSpec
import pyspark.sql.functions as F
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
conf = pyspark.SparkConf()
from tqdm import tqdm
tqdm.pandas()

import warnings
warnings.filterwarnings("ignore")

sns.set_style("darkgrid")

# COMMAND ----------

from pymongo import MongoClient
MONGO_CONN = 'mongodb+srv://<username>:<password>@retail-demo.2wqno.mongodb.net/?retryWrites=true&w=majority'
client = MongoClient(MONGO_CONN)

# COMMAND ----------

conf.set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.0")

spark = SparkSession.builder \
        .master("local") \
        .appName("pricing") \
        .config(conf = conf) \
        .getOrCreate()

# COMMAND ----------

pipeline = [
    {
        '$group': {
            '_id': {
                'product_id': '$product_id', 
                'event_date': {
                    '$dateToString': {
                        'format': '%Y-%m-%d', 
                        'date': {
                            '$dateFromString': {
                                'dateString': '$event_time'
                            }
                        }
                    }
                }
            }, 
            'product_id': {
                '$first': '$product_id'
            }, 
            'event_date': {
                '$first': {
                    '$dateToString': {
                        'format': '%Y-%m-%d', 
                        'date': {
                            '$dateFromString': {
                                'dateString': '$event_time'
                            }
                        }
                    }
                }
            }, 
            'avg_price': {
                '$avg': {
                    '$toDouble': '$price'
                }
            }, 
            'max_price': {
                '$max': {
                    '$toDouble': '$price'
                }
            }, 
            'min_price': {
                '$min': {
                    '$toDouble': '$price'
                }
            }, 
            'total_sales': {
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
            }
        }
    }, {
        '$project': {
            '_id': 0
        }
    }
]
clogs=spark.read.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "clogs").\
            option('spark.mongodb.aggregation.pipeline',pipeline).\
            option("forceDeleteTempCheckpointLocation", "true").load()
display(clogs)

# COMMAND ----------

windowSpec = Window.partitionBy('product_id').orderBy('event_date')
clogs = clogs.withColumn('old_sales', F.lag(clogs['total_sales'], offset=1).over(windowSpec))
clogs = clogs.withColumn('old_avg_price', F.lag(clogs['avg_price'], offset=1).over(windowSpec))

# COMMAND ----------

mapping=spark.readStream.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "mapping_n").\
            option('spark.mongodb.aggregation.pipeline',[{"$project":{"_id":0}}]).\
            option("forceDeleteTempCheckpointLocation", "true").load()
clogs = clogs.join(F.broadcast(mapping),on='product_id',how='inner')

# COMMAND ----------

clogs.write.format("mongodb").\
            option('spark.mongodb.connection.uri', MONGO_CONN).\
            option('spark.mongodb.database', "search").\
            option('spark.mongodb.collection', "processed_clogs_n").\
            option('spark.mongodb.operationType', "insert").\
            mode('append').\
            save()

# COMMAND ----------

# DBTITLE 1,Read From processed collection for train data
db = client['search']
collection = db['processed_clogs_n']
t = pd.DataFrame.from_records(collection.aggregate([{"$sample":{"size":100000}}]))

# COMMAND ----------

from scipy.optimize import fmin,fminbound
from multiprocessing import Pool

def cross_price_elasticity(qa1, qa2, pb1, pb2):
    # qa1 = initial quantity demanded of product A
    # qa2 = new quantity demanded of product A
    # pb1 = initial price of product B
    # pb2 = new price of product B
    # Calculate the percentage change in demand of product A
    try:
        percent_change_demand_a = (qa2 - qa1) / ((qa1 + qa2) / 2)
        # Calculate the percentage change in price of product B
        percent_change_price_b = (pb2 - pb1) / ((pb1 + pb2) / 2)
        # Calculate cross price elasticity
        ced = percent_change_demand_a / percent_change_price_b 
        return ced
    except:
        return 0

features = []
price = []

def optimize_price(row):
    fn = lambda x: cross_price_elasticity(row['old_sales'],row['total_sales'] , row['old_avg_price'],x) + ((row['max_price'] - x) - 0.1)**2
    # try:
    #     return fminbound(fn, 1,row['max_price']*1.2)
    # except:
    #     return row['avg_price']
    return fmin(fn, row['max_price'], xtol=1e-8, ftol=1e-3)[0]

t['price'] = t.progress_apply(optimize_price,axis=1)
nprice = t[t.price<0]['max_price']
t.loc[t.price<0,'price'] = nprice


# COMMAND ----------

import pandas as pd
import numpy as np
import sklearn
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

X = t[["old_sales", "total_sales", "min_price", "max_price", "avg_price",'old_avg_price']]
y = t["price"]

# Split data into train and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.01)

# COMMAND ----------

import mlflow
import mlflow
import mlflow.sklearn
import mlflow.pyfunc
import mlflow.spark
from mlflow.models.signature import infer_signature
from mlflow.utils.environment import _mlflow_conda_env
# init mlflow wrapper for XGB model
class CompPriceModelWrapper(mlflow.pyfunc.PythonModel):
    def __init__(self, model):
        self.model = model

    def predict(self,context,model_input):
        return self.model.predict(model_input)

# COMMAND ----------

model_name = "retail_competitive_pricing_model_1"
with mlflow.start_run(run_name=model_name):
    # Create and fit a linear regression model
    model = RandomForestRegressor(n_estimators=50)
    model.fit(X_train, y_train)
    wrappedModel = CompPriceModelWrapper(model)

    # Log model parameters and metrics
    mlflow.log_params(model.get_params())
    mlflow.log_metric("mse", np.mean((model.predict(X_test) - y_test) ** 2))
    
    # Log the model with a signature that defines the schema of the model's inputs and outputs. 
    # When the model is deployed, this signature will be used to validate inputs.
    signature = infer_signature(X_train, wrappedModel.predict(None,X_train))

    # MLflow contains utilities to create a conda environment used to serve models.
    # The necessary dependencies are added to a conda.yaml file which is logged along with the model.
    conda_env =  _mlflow_conda_env(
        additional_conda_deps=None,
        additional_pip_deps=["scikit-learn=={}".format(sklearn.__version__)],
        additional_conda_channels=None,
    )
    mlflow.pyfunc.log_model(model_name, python_model=wrappedModel, conda_env=conda_env, signature=signature)

# COMMAND ----------

# DBTITLE 1,Register trained model
run_id = mlflow.search_runs(filter_string='tags.mlflow.runName="retail_competitive_pricing_model_1"').iloc[0].run_id
model_version = mlflow.register_model(f"runs:/{run_id}/{model_name}", model_name)

# COMMAND ----------

# DBTITLE 1,Push to staging env
print(f"Publishing the {model_name}-{run_id} to Staging env")
from mlflow.tracking import MlflowClient
client = MlflowClient()
client.transition_model_version_stage(name=model_name,version=model_version.version,stage="Staging")

# COMMAND ----------

X_train.columns.tolist()

# COMMAND ----------

