# -*- coding: utf-8 -*-
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from etl_tools import write_s3
from api_tools import fetch_scodify_api_records
from scodify_connector import ScodifyApiConnector


# CONTEXT
args = getResolvedOptions(sys.argv, ["JOB_NAME", "environment"])
sc = SparkContext()
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session
job.init(args["JOB_NAME"], args)
env = args["environment"]
db = 'bronze'
scodify = ScodifyApiConnector(env)
desti = 'scodify_project'

# EXTRACT

# Liste des id à envoyer à scodify

conso_plan = spark.sql(f"""
SELECT
    idplan
FROM {env}_snowlake_bronze.scream_conso_plan
""")
idsplan = conso_plan.select("idplan").rdd.map(lambda x: x[0]).collect()

# Appeler l'API de scodify en donnant la liste d'idsplan

df = None
if len(idsplan) > 0:
    data = {"mapIds": idsplan}
    df = fetch_scodify_api_records(scodify, 'private/projects', data)

# LOAD
if df is not None:
    df = spark.createDataFrame(df)
    write_s3(spark=spark,
             df=df,
             env=env,
             db=db,
             table=desti,
             method='overwrite',
             partition_cols=[],
             path_prefix='scodify/')
job.commit()
