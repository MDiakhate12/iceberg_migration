# -*- coding: utf-8 -*-
import io
import pandas as pd
import requests
import sys
import zipfile
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from etl_tools import write_s3, get_attr_val


# CONTEXT
args = getResolvedOptions(sys.argv, ["JOB_NAME", "environment"])
sc = SparkContext()
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session
job.init(args["JOB_NAME"], args)
env = args["environment"]
db = 'bronze'
desti = 'flydoc_ondemand'

max_bronze = get_attr_val(env=env, db=db, table=desti, col='dt', func='max') or '2024-01-01'

if env == 'prod':
    links = spark.sql(f"""
    SELECT
        dt
        , url
    FROM {env}_snowlake_external.flydoc_links
    WHERE dt > '{max_bronze}'
    """)
else:  # Mesure d'économie
    links = spark.sql(f"""
    SELECT
        dt
        , url
    FROM {env}_snowlake_external.flydoc_links
    WHERE dt > '{max_bronze}'
    ORDER BY dt DESC
    LIMIT 1
    """)

for link in links.select('url').rdd.flatMap(lambda x: x).collect():
    print(link)
    response = requests.get(link)
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        file_list = z.namelist()
        for file_name in file_list:
            if file_name.startswith('Mail on Demand'):
                with z.open(file_name) as f:
                    df = pd.read_csv(f, sep=';', encoding='latin1', low_memory=False)
                    df = df.astype(str)
                    df['_ingest'] = pd.Timestamp.now()
                    df['_src'] = file_name
                    sf = spark.createDataFrame(df)

                    write_s3(
                        spark=spark,
                        df=sf,
                        env=env,
                        db=db,
                        table=desti,
                        method='append',
                        partition_cols=['_src']
                    )

job.commit()
