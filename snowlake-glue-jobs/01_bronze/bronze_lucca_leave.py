# -*- coding: utf-8 -*-
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from etl_tools import write_s3
import requests
import json
import pandas as pd


# CONTEXT
args = getResolvedOptions(sys.argv, ["JOB_NAME", "environment"])
sc = SparkContext()
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session
job.init(args["JOB_NAME"], args)
env = args["environment"]
db = 'bronze'
desti = 'lucca_leave'
date_min = 2020
# Clé d'API Lucca
api_key = "249136b5-5324-4acb-8906-7e9e93ffe4d7"

# Entête HTTP avec la clé d'API
headers = {'Accept': 'application/json',
           'Authorization': 'lucca application={' + api_key + '}'}

# EXTRACT
# URL de l'API Lucca pour la liste des départements
url_departments = "https://sogelink.ilucca.net/api/v3/departments?fields=id,name,hierarchy,parentId,level"

# Envoi d'une requête HTTP GET à l'URL de l'API
response = requests.get(url_departments, headers=headers)

# Convertion de la réponse JSON en objet Python
data = json.loads(response.text)

# Concaténation des idDepartment pour l'utiliser dans la requête des absences
all_departments = ""
for department in data["data"]["items"]:
    all_departments = all_departments + "," + str(department["id"])

all_departments = all_departments[1:]  # Suppression de la première virgule

# URL de l'API Lucca pour les absences des employés en ajoutant la variable de la liste des départements
url_leaves = f"https://sogelink.ilucca.net/api/v3/leaves?leavePeriod.owner.departmentId={all_departments}&isActive=true&date=since,{date_min}-01-01&fields=id,date,isAM,scope,workingTimeType,calendar,leaveAccountDuration,leavePeriod[id,isConfirmed,owner[id,lastName,firstName,displayName,mail,department[id,name]]],leaveAccount[id,name],isRealLeave"

# Envoi d'une requête HTTP GET à l'URL de l'API
response = requests.get(url_leaves, headers=headers)

# Convertion de la réponse JSON en objet Python
data = json.loads(response.text)

# Convertion en DataFrame
absences = pd.json_normalize(data["data"]["items"])

# Convertion en Spark
s_absences = spark.createDataFrame(absences)

# LOAD
write_s3(spark=spark,
         df=s_absences,
         env=env,
         db=db,
         table=desti,
         method='overwrite',
         partition_cols=[],
         path_prefix='lucca/')
job.commit()
