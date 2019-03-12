with open(secrets_path) as json_data:
    writeConfig = json.load(json_data)
    recs = model.recommendForAllUsers(10)
    recs.withColumn("id",recs[userCol].cast("string")).select("id", "recommendations."+ itemCol)\
    .write.format("com.microsoft.azure.cosmosdb.spark").mode('overwrite').options(**writeConfig).save()
    
score_sparkml = """

import json
def init(local=False):
    global client, collection
    try:
      # Query them in SQL
      import pydocumentdb.document_client as document_client

      MASTER_KEY = '{key}'
      HOST = '{endpoint}'
      DATABASE_ID = "{database}"
      COLLECTION_ID = "{collection}"
      database_link = 'dbs/' + DATABASE_ID
      collection_link = database_link + '/colls/' + COLLECTION_ID
      
      client = document_client.DocumentClient(HOST, {'masterKey': MASTER_KEY})
      collection = client.ReadCollection(collection_link=collection_link)
    except Exception as e:
      collection = e
def run(input_json):      

    try:
      import json

      id = json.loads(json.loads(input_json)[0])['id']
      query = {'query': 'SELECT * FROM c WHERE c.id = "' + str(id) +'"' } #+ str(id)

      options = {'partitionKey':str(id)}
      document_link = 'dbs/{DOCUMENTDB_DATABASE}/colls/{DOCUMENTDB_COLLECTION}/docs/{0}'.format(id)
      result = client.ReadDocument(document_link, options);
  
    except Exception as e:
        result = str(e)
    return json.dumps(str(result)) #json.dumps({{"result":result}})
"""


with open(secrets_path) as json_data:
    writeConfig = json.load(json_data)
    score_sparkml = score_sparkml.replace("{key}",writeConfig['Masterkey']).replace("{endpoint}",writeConfig['Endpoint']).replace("{database}",writeConfig['Database']).replace("{collection}",writeConfig['Collection']).replace("{DOCUMENTDB_DATABASE}",DOCUMENTDB_DATABASE).replace("{DOCUMENTDB_COLLECTION}", DOCUMENTDB_COLLECTION)

    exec(score_sparkml)

    with open("score_sparkml.py", "w") as file:
        file.write(score_sparkml)    
        
mymodel = Model.register(model_path = model_name, # this points to a local file
                       model_name = model_name, # this is the name the model is registered as, am using same name for both path and name.                 
                       description = "ADB trained model",
                       workspace = ws)

print(mymodel.name, mymodel.description, mymodel.version)        

# Create Image for Web Service
models = [mymodel]
runtime = "spark-py"
conda_file = 'myenv_sparkml.yml'
driver_file = "score_sparkml.py"

# image creation
from azureml.core.image import ContainerImage
myimage_config = ContainerImage.image_configuration(execution_script = driver_file, 
                                    runtime = runtime, 
                                    conda_file = conda_file)

image = ContainerImage.create(name = container_image_name,
                                # this is the model object
                                models = [mymodel],
                                image_config = myimage_config,
                                workspace = ws)

# Wait for the create process to complete
image.wait_for_creation(show_output = True)


from azureml.core.compute import AksCompute, ComputeTarget

# Use the default configuration (can also provide parameters to customize)
prov_config = AksCompute.provisioning_configuration()

# Create the cluster
aks_target = ComputeTarget.create(workspace = ws, 
                                  name = aks_name, 
                                  provisioning_configuration = prov_config)

aks_target.wait_for_completion(show_output = True)

print(aks_target.provisioning_state)
print(aks_target.provisioning_errors)

#Set the web service configuration (using default here with app insights)
aks_config = AksWebservice.deploy_configuration(enable_app_insights=True)

# Webservice creation using single command, there is a variant to use image directly as well.
try:
    aks_service = Webservice.deploy_from_image(
      workspace=ws, 
      name=service_name,
      deployment_config = aks_config,
      image = image,
      deployment_target = aks_target
      )
    aks_service.wait_for_deployment(show_output=True)
except Exception:
    aks_service = Webservice.list(ws)[0]
    

scoring_url = aks_service.scoring_uri
service_key = aks_service.get_keys()[0]

input_data = '["{\\"id\\":\\"496\\"}"]'.encode()

req = urllib.request.Request(scoring_url,data=input_data)
req.add_header("Authorization","Bearer {}".format(service_key))
req.add_header("Content-Type","application/json")

tic = time.time()
with urllib.request.urlopen(req) as result:
    res = result.readlines()
    print(res)
    
toc = time.time()
t2 = toc - tic
print("Full run took %.2f seconds" % (toc - tic))    
