# -*- coding: utf-8 -*-
# %%
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark import SparkConf
import etl_tools
import datasources
import snowlake_api_tools
import table_tools
from datetime import datetime, timedelta
import pyspark.sql.functions as F
import logging
from concurrent.futures import ThreadPoolExecutor


# JOB CONTEXT SETUP
args = etl_tools.get_args()
env = args["environment"]
conf = SparkConf()

conf = etl_tools.configure_iceberg(
    env,
    "bronze",
)

sc = SparkContext.getOrCreate(conf=conf)
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session
job.init(args["JOB_NAME"], args)

catalog = spark.conf.get("spark.environment.catalog_name")
database = spark.conf.get("spark.environment.database_name")

# %%


def get_record_count(datasource, start_date=None, end_date=None, date_column=None):
    connector = snowlake_api_tools.FocusApiConnector()

    params = {
        "pageNumber": "1",
        "ledger": "1",
        "fromPeriod": start_date,
        "toPeriod": end_date,
    }

    data = connector.get(datasource.url_path, params=params)

    logging.info("Fetching record count...")

    if not data:
        print("No records found from ")
        return 0

    return int(data[0]["metadata"]["totalCount"])


def collect_primary_keys(datasource, start_date=None, end_date=None, date_column=None):
    connector = snowlake_api_tools.FocusApiConnector()
    record_count = get_record_count(datasource, start_date, end_date, date_column)

    logging.info(f"Total records to process: {record_count}")
    params = {
        "pageNumber": "1",
        "ledger": "1",
        "fromPeriod": start_date,
        "toPeriod": end_date
    }
    connector = snowlake_api_tools.FocusApiConnector()
    data = connector.get(datasource.url_path, params=params)

    logging.info(f"Get total number of pages for {datasource.source_table_alias}..")
    page_size = data[0]["metadata"]["maxPageSize"]
    number_of_pages = (record_count // page_size) + 1

    page_range = range(2, number_of_pages + 1)
    primary_keys = []

    def fetch(page):

        logging.info(f"Fetching page {page} for {datasource.source_table_alias}")

        params["pageNumber"] = page

        data = connector.get(datasource.url_path, params=params)

        primary_keys.extend([tuple(row[elem] for elem in datasource.primary_keys) for row in data])
    with ThreadPoolExecutor(max_workers=60) as executor:
        list(executor.map(fetch, page_range))

    return primary_keys


# %%
# Retrieve dates from args or calculate default values

if etl_tools.is_running_locally():
    start_date = input("Enter start yearmonth (yyyymm) or leave empty for default: ")
    end_date = input("Enter end yearmonth (yyyymm) or leave empty for default: ")
    table = input("Enter table name: ")
else:
    today = datetime.today()
    first_day_of_last_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    start_date = first_day_of_last_month.strftime("%Y%m")
    end_date = today.strftime("%Y%m")


print(f"Default start date: {start_date}, end date: {end_date}")

start_date = args.get("start_date", start_date)
end_date = args.get("end_date", end_date)

table = args.get("table", table)
datasource = datasources.VismaDataSources.get_one(table)
# %%
print(f"Using start date: {start_date}, end date: {end_date} for table: {table}")

print(f"Collecting source primary keys for {datasource.source_table_alias} from {start_date} to {end_date}")
primary_keys = collect_primary_keys(
    datasource=datasource,
    start_date=start_date,
    end_date=end_date,
    date_column=datasource.datetime_column,
)
print(primary_keys)
# %%
print(f"Collecting target primary keys for {datasource.source_table_alias}.")
target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")

target_primary_keys = (
    target_df
    .where(F.col("period") >= F.lit(start_date))
    .where(F.col("period") <= F.lit(end_date))
    .select(*datasource.primary_keys)
    .distinct()
    .collect()
)

target_primary_keys = [
    tuple(elem for elem in row)
    for row in target_primary_keys
]

deleted_primary_keys = set(target_primary_keys) - set(primary_keys)

print(f"Collected {len(list(set(primary_keys)))} primary keys in source between {start_date} and {end_date}")

print(f"Collected {len(list(set(target_primary_keys)))} primary keys in target")

print(f"Found {len(deleted_primary_keys)} primary keys in target but not in source (might be missing)")

# %%
print(primary_keys)
# %%

number_of_deleted_primary_keys = len(deleted_primary_keys)
deactivate_scd2 = f", _is_active = FALSE, _end_date = {datetime.now()}" if datasource.write_mode == "SCD2" else ""
target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")

if "_is_deleted" not in target_df.columns:
    print("'_is_deleted' non existent, adding it to the target table...")
    create_is_deleted_column_query = f"""
        ALTER TABLE {catalog}.{database}.{datasource.target_table_name}
            ADD COLUMN _is_deleted BOOLEAN
    """
    print(f"Executing query to add '_is_deleted' column: {create_is_deleted_column_query}")
    spark.sql(create_is_deleted_column_query)

    initialize_is_deleted_query = f"""
        UPDATE {catalog}.{database}.{datasource.target_table_name}
        SET _is_deleted = FALSE
    """
    print(f"Executing query to initialize '_is_deleted' column: {initialize_is_deleted_query}")
    spark.sql(initialize_is_deleted_query)

# %%
deleted_primary_keys_df = spark.createDataFrame(deleted_primary_keys, schema=datasource.primary_keys)

deleted_rows_df = (
    target_df.alias("t")
    .join(deleted_primary_keys_df.alias("d"), on=datasource.primary_keys, how="inner")
    .select("t.*")
)

deleted_rows_df = deleted_rows_df.withColumn(
    "_is_deleted", F.lit(True)
)

if datasource.write_mode == "SCD2":
    deleted_rows_df = deleted_rows_df.withColumn("_is_active", F.lit(False))
# %%
tmp_view_name = "deleted_rows_temp_view"

deleted_rows_df.createOrReplaceTempView(tmp_view_name)

print(f"Updating {number_of_deleted_primary_keys} rows from target table {datasource.target_table_name} to mark them as missing...")
merge_query = f"""
    MERGE INTO {catalog}.{database}.{datasource.target_table_name} AS target
    USING {tmp_view_name} AS source
    ON {table_tools.create_merge_condition(datasource.primary_keys)}
    WHEN MATCHED THEN
        UPDATE SET
            _is_deleted = TRUE
            {deactivate_scd2}
"""

print(f"Executing delete query: {merge_query}")
spark.sql(merge_query)
