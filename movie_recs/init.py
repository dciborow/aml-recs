import numpy as np
import os
import pandas as pd
import pprint
import shutil
import time, timeit
import urllib
import yaml
import json
import uuid
import matplotlib
import matplotlib.pyplot as plt

from azure.common.client_factory import get_client_from_cli_profile
from azure.mgmt.compute import ComputeManagementClient
import azure.mgmt.cosmosdb
import azureml.core
from azureml.core import Workspace
from azureml.core.run import Run
from azureml.core.experiment import Experiment
from azureml.core.model import Model
from azureml.core.image import ContainerImage
from azureml.core.compute import AksCompute, ComputeTarget
from azureml.core.webservice import Webservice, AksWebservice


import pydocumentdb
import pydocumentdb.document_client as document_client

import pyspark
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, FloatType, IntegerType, LongType

from reco_utils.dataset import movielens
from reco_utils.dataset.cosmos_cli import find_collection, read_collection, read_database, find_database
from reco_utils.dataset.spark_splitters import spark_random_split
from reco_utils.evaluation.spark_evaluation import SparkRatingEvaluation, SparkRankingEvaluation

print("PySpark version:", pyspark.__version__)
print("Azure SDK version:", azureml.core.VERSION)

# Select the services names
short_uuid = str(uuid.uuid4())[:4]
prefix = "reco" + short_uuid
data = "mvl"
algo = "als"

# location to store the secrets file for cosmosdb
secrets_path = '/dbfs/FileStore/dbsecrets.json'
ws_config_path = '/dbfs/FileStore'

# Add your subscription ID
subscription_id = ""

# Resource group and workspace
resource_group = prefix + "_" + data
workspace_name = prefix + "_"+data+"_aml"
workspace_region = "westus2"
print("Resource group:", resource_group)

# Columns
userCol = "UserId"
itemCol = "MovieId"
ratingCol = "Rating"

# CosmosDB
location = workspace_region
account_name = resource_group + "-ds-sql"
# account_name for CosmosDB cannot have "_" and needs to be less than 31 chars
account_name = account_name.replace("_","-")[0:min(31,len(prefix))]
DOCUMENTDB_DATABASE = "recommendations"
DOCUMENTDB_COLLECTION = "user_recommendations_" + algo

# AzureML
history_name = 'spark-ml-notebook'
model_name = data+"-"+algo+"-reco.mml" #NOTE: The name of a asset must be only letters or numerals, not contain spaces, and under 30 characters
service_name = data + "-" + algo
experiment_name = data + "_"+ algo +"_Experiment"
# Name here must be <= 16 chars and only include letters, numbers and "-"
aks_name = prefix.replace("_","-")[0:min(12,len(prefix))] + '-aks'
# add a name for the container
container_image_name = '-'.join([data, algo])

train_data_path = data + "Train"
test_data_path = data + "Test"

ws = Workspace.create(name = workspace_name,
                      subscription_id = subscription_id,
                      resource_group = resource_group, 
                      location = workspace_region,
                      exist_ok=True)

# persist the subscription id, resource group name, and workspace name in aml_config/config.json.
ws.write_config(ws_config_path)

## explicitly pass subscription_id in case user has multiple subscriptions
client = get_client_from_cli_profile(azure.mgmt.cosmosdb.CosmosDB,
                                    subscription_id=subscription_id)

async_cosmosdb_create = client.database_accounts.create_or_update(
    resource_group,
    account_name,
    {
        'location': location,
        'locations': [{
            'location_name': location
        }]
    }
)
account = async_cosmosdb_create.result()

my_keys = client.database_accounts.list_keys(
    resource_group,
    account_name
)

master_key = my_keys.primary_master_key
endpoint = "https://" + account_name + ".documents.azure.com:443/"

#db client
client = document_client.DocumentClient(endpoint, {'masterKey': master_key})

if find_database(client, DOCUMENTDB_DATABASE) == False:
    db = client.CreateDatabase({ 'id': DOCUMENTDB_DATABASE })
else:
    db = read_database(client, DOCUMENTDB_DATABASE)
# Create collection options
options = {
    'offerThroughput': 11000
}

# Create a collection
collection_definition = { 'id': DOCUMENTDB_COLLECTION, 'partitionKey': {'paths': ['/id'],'kind': 'Hash'} }
if find_collection(client,DOCUMENTDB_DATABASE,  DOCUMENTDB_COLLECTION) ==False:
    collection = client.CreateCollection(db['_self'], collection_definition, options)
else:
    collection = read_collection(client, DOCUMENTDB_DATABASE, DOCUMENTDB_COLLECTION)
    

secrets = {
  "Endpoint": endpoint,
  "Masterkey": master_key,
  "Database": DOCUMENTDB_DATABASE,
  "Collection": DOCUMENTDB_COLLECTION,
  "Upsert": "true"
}
with open(secrets_path, "w") as file:
    json.dump(secrets, file)    
