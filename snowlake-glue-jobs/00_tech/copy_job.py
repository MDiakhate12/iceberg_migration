# -*- coding: utf-8 -*-
# %%
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from etl_tools import write_s3, get_args, configure_iceberg


logger = logging.getLogger(__name__)

# JOB CONTEXT SETUP

copy_info = {
    "table": "netsuite_transactionline",
    "source_env": "prod",
    "target_env": "inte",
    "source_db": "bronze",
    "target_db": "bronze",
}

args = get_args(env=copy_info["target_env"])

conf = configure_iceberg(
    env=args["environment"],
    db=copy_info["target_db"]
)

sc = SparkContext(conf=conf)
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session

job.init(args["JOB_NAME"], args)

logger = logging.getLogger(__name__)

catalog = spark.conf.get("spark.environment.catalog_name")

# %%

source_table_name = f"{copy_info['source_env']}_snowlake_{copy_info['source_db']}.{copy_info['table']}"

if "format" in copy_info:
    source_table_name = f"{catalog}.{source_table_name}"

logger.info(f"Copying {source_table_name} from {copy_info['source_env']}/{copy_info['source_db']} to {copy_info['target_env']}/{copy_info['target_db']}")
# %%
source_table = spark.table(source_table_name).limit(100)
# %%
write_s3(
    spark=spark,
    df=source_table,
    env=copy_info['target_env'],
    db=copy_info['target_db'],
    table=copy_info['table'],
    method='overwrite',
    path_prefix=f"{copy_info['target_db']}/",
    partition_cols=[]
)
# %%
