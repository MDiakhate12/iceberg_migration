# -*- coding: utf-8 -*-
# %%
import logging
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import etl_tools  # alway import elt_tools before other modules that uses the logger
import snowlake_api_tools
import table_tools
import datasources

args = etl_tools.get_args()
conf = etl_tools.configure_iceberg(
    env=args["environment"],
    db="bronze",
    folder="visma",
)

conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

sc = SparkContext(conf=conf)
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session

job.init(args["JOB_NAME"], args)

logger = logging.getLogger(__name__)

PARQUET_SIZE_MB_MAX = 512

catalog = spark.conf.get("spark.environment.catalog_name")
database = spark.conf.get("spark.environment.database_name")
s3_path = spark.conf.get("spark.environment.data_s3_path")

# Connect
connector = snowlake_api_tools.FocusApiConnector()

# %%

# Ingest
datasource = datasources.VismaDataSources.INVENTORY
logger.info(f"Table to ingest : {datasource}")

table_exists = etl_tools.check_table_exists(
    gc=gc,
    env=args["environment"],
    db="bronze",
    table=datasource.target_table_name,
)

s3_tmp_path = f"{s3_path}/tmp/{datasource.source_table_alias}.json"

if args.get("replay_from_tmp", False) is True:

    logger.warning(f"Replaying ingestion from tmp files in s3 {s3_tmp_path} ! "
                   "Make sure these files are up to date")

    logger.warning("Never run a replay in production !")

else:
    if table_exists:
        target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")
    else:
        logger.info(f"Getting all data of {datasource.source_table_alias}...")
        data = connector.get(datasource.url_path)
        etl_tools.write_json(data, s3_tmp_path)

input_df = spark.read.option("multiline", "true").json(s3_tmp_path)

# Try to compress to 256MB per parquet file
logger.info("Make sure we don't have small files : target is ~256MB per parquet fiLe")
coalesced_df = etl_tools.coalesce_dataframe(input_df, target_parquet_size_mb=PARQUET_SIZE_MB_MAX)

# Reorder dataframe
other_columns = [c for c in input_df.columns if c not in datasource.primary_keys]
reodered_df = input_df.select(datasource.primary_keys + other_columns)

# Add metadata
final_df = etl_tools.add_meta(reodered_df)

if table_exists:
    target_schema_diff = {c for c in set(target_df.schema) - set(final_df.schema) if not c.name.startswith("_")}
    if target_schema_diff:
        logger.warning(f"Some columns have been changed or removed {target_schema_diff}")

    source_schema_diff = {c for c in set(final_df.schema) - set(target_df.schema) if not c.name.startswith("_")}
    if source_schema_diff:
        logger.warning(f"Some columns have been changed or added {source_schema_diff}")

# %%

# Write table to iceberg
table_tools.write_iceberg(
    df=final_df,
    primary_keys=datasource.primary_keys,
    table_name=datasource.target_table_name,
    glue_context=gc,
    spark=spark,
    write_mode=table_tools.WriteMode.SCD2,
    tableProperties={
        'write.spark.accept-any-schema': 'true'
    },
    options={
        "mergeSchema": "true"
    },
)

job.commit()

# %%
