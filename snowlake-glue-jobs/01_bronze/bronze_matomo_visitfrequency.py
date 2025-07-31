# -*- coding: utf-8 -*-
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from etl_tools import write_s3
from api_tools import fetch_matomo_api_records
from matomo_connector import MatomoApiConnector


# CONTEXT
args = getResolvedOptions(sys.argv, ["JOB_NAME", "environment"])
sc = SparkContext()
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session
job.init(args["JOB_NAME"], args)
env = args["environment"]
db = 'bronze'
mato = MatomoApiConnector(id_site=5)
desti = 'matomo_visitfrequency'
method = 'append'

# EXTRACT

df = fetch_matomo_api_records(spark, env, db, mato, "VisitFrequency.get", None, desti, method)

# LOAD
write_s3(spark=spark,
         df=df,
         env=env,
         db=db,
         table=desti,
         method=method,
         partition_cols=['_year', '_month', '_day'],
         path_prefix='matomo/')
job.commit()
