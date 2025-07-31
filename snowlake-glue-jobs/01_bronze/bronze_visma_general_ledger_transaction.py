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
import pyspark.sql.functions as F
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


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
PAGE_SAFETY_OFFSET = 7

catalog = spark.conf.get("spark.environment.catalog_name")
database = spark.conf.get("spark.environment.database_name")
s3_path = spark.conf.get("spark.environment.data_s3_path")

# %%

# Connect
connector = snowlake_api_tools.FocusApiConnector()


def fetch_and_save_to_s3(page, datasource, params):

    filename = f"{datasource.source_table_alias}_{page}"
    tmp_json_path = f"{s3_path}/tmp/{filename}.json"

    logger.info(f"Fetching page {page} for {datasource.source_table_alias}")

    params["pageNumber"] = page

    data = connector.get(datasource.url_path, params=params)

    # Write to a tmp directory
    logger.info(f"Write a tmp file into s3 for further processing {filename}.json to {tmp_json_path}")
    etl_tools.write_json(data, tmp_json_path)

    return tmp_json_path


# Ingest
datasource = datasources.VismaDataSources.GENERALLEDGERTRANSACTION
logger.info(f"Table to ingest : {datasource}")

json_files = []

table_exists = etl_tools.check_table_exists(
    gc=gc,
    env=args["environment"],
    db="bronze",
    table=datasource.target_table_name,
)

if table_exists:
    target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")

# %%

# Fetch first page
now = datetime.now()
year_month = int(now.strftime("%Y%m"))
params = {
    "pageNumber": "1",
    "ledger": "1",
    "fromPeriod": "202301",
    "toPeriod": year_month
}
data = connector.get(datasource.url_path, params=params)

# Get number of pages from metadata
logger.info(f"Get total number of pages for {datasource.source_table_alias}..")
total_lines = data[0]["metadata"]["totalCount"]
page_size = data[0]["metadata"]["maxPageSize"]

number_of_pages = (total_lines // page_size) + 1
# number_of_pages = (total_lines // page_size) + 1

logger.info(f"There is a total of {total_lines} ({page_size} per page) "
            f"in {datasource.source_table_alias} = {number_of_pages} pages")

first_file_path = f"{s3_path}/tmp/{datasource.source_table_alias}_1.json"
etl_tools.write_json(data, first_file_path)
json_files.append(first_file_path)

page_range = range(2, number_of_pages + 1)

with ThreadPoolExecutor(max_workers=10) as executor:
    file_keys = list(
        executor.map(
            lambda page: fetch_and_save_to_s3(page, datasource, params),
            list(page_range)
        )
    )

logger.info(page_range)
logger.info(file_keys)

json_files.extend(file_keys)


# Try to compress to 256MB per parquet file
logger.info("Make sure we don't have small files : target is ~256MB per parquet fiLe")

# Write json
# %%
input_df = (
    spark.read.option("multiline", "true")
    .json(json_files)
    .withColumn(
        "_page_number",
        F.regexp_extract(F.input_file_name(), fr"{datasource.source_table_alias}_(\d+)\.json", 1)
    )
)

coalesced_df = etl_tools.coalesce_dataframe(input_df, target_parquet_size_mb=PARQUET_SIZE_MB_MAX)
other_columns = [c for c in coalesced_df.columns if c not in datasource.primary_keys]

reodered_df = coalesced_df.select(datasource.primary_keys + other_columns)

# Drop duplicates as placeholder fix
# dedup_df = reodered_df.dropDuplicates(datasource.primary_keys)

final_df = reodered_df

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
    write_mode=table_tools.WriteMode.UPSERT,
    tableProperties={
        'write.spark.accept-any-schema': 'true'
    },
    options={
        "mergeSchema": "true"
    },
)

job.commit()
