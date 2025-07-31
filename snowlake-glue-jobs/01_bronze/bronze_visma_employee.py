# -*- coding: utf-8 -*-
import pandas as pd
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from etl_tools import write_s3, get_attr_val
from api_tools import fetch_visma_api_records
from focus_connector import FocusApiConnector

# CONTEXT
args = getResolvedOptions(sys.argv, ["JOB_NAME", "environment"])
sc = SparkContext()
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session
job.init(args["JOB_NAME"], args)
env = args["environment"]

api = "https://api.focus.no/vismareadproxy/v1"
token = 'a9be759b641647b09333a1637bba5510'
tribe = FocusApiConnector(api, token)

dt_min = '2024-01-01T00:00:00.000'
db = 'bronze'
desti = 'visma_employee'

# EXTRACT
dt_max = get_attr_val(env=env, db=db, table=desti, col='lastModifiedDateTime', func='max') or dt_min
q = f"employee?lastModifiedDateTime after '{dt_max}'"
df = fetch_visma_api_records(tribe, q)
# df

if not df.empty:
    df.columns = [c.lower() for c in df.columns]
    df['_ingest'] = pd.Timestamp.now()

    df = spark.createDataFrame(df)
    write_s3(spark=spark,
             df=df,
             env=env,
             db=db,
             table=desti,
             method='append',
             partition_cols=['_ingest'],
             path_prefix='visma/')

job.commit()
