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


def collect_primary_keys(datasource, start_date=None, date_column=None):
    connector = snowlake_api_tools.AfasApiConnector()
    pagination_strategy = snowlake_api_tools.OffsetPagination(
        page_size=datasource.page_size,
        limit_param="take",
        offset_param="skip",
    )

    params = {
            "filterfieldids": "Jaar,Periode",
            "filtervalues": f"{start_date.year},{start_date.month}",
            "orderbyfieldids": "Jaar,Periode",
            "operatortypes": "2,2"
    }
    primary_keys = []
    results_generator = connector.fetch_pages_lazy(
        url=datasource.url_path,
        pagination_strategy=pagination_strategy,
        data_key="rows",
        params=params,
        source_name=datasource.source_table_alias,
    )

    for data in results_generator:
        primary_keys.extend([tuple(item[k] for k in datasource.primary_keys) for item in data])
        print(primary_keys)

    return primary_keys


# %%
# Retrieve dates from args or calculate default values
if etl_tools.is_running_locally():
    start_date = datetime.strptime(input("Enter start date (yyyydd) or leave empty for default: ").strip(), "%Y%m")
    print(start_date.year)
    table = input("Enter table name: ")
else:
    today = datetime.today()
    first_day_of_last_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    start_date = first_day_of_last_month.strftime("%Y%m")


start_year = start_date.year
start_month = start_date.month

print(f"Default start date: {start_date}")

start_date = args.get("start_date", start_date)
table = args.get("table", table)
datasource = datasources.AfasDataSources.get_one(table)
# %%
print(f"Using start date: {start_date} for table: {table}")

print(f"Collecting source primary keys for {datasource.source_table_alias} from {start_date}")
primary_keys = collect_primary_keys(
    datasource=datasource,
    start_date=start_date,
    date_column=datasource.datetime_column,
)

# %%
print(f"Collecting target primary keys for {datasource.source_table_alias}.")
target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")

pk_df = target_df.select(*datasource.primary_keys)

target_primary_keys = (
    pk_df
    .where(F.col("Jaar") >= F.lit(start_year))
    .where(F.col("Periode") >= F.lit(start_month))
    .distinct()
    .collect()
)

target_primary_keys = [
    tuple(row[col] for col in datasource.primary_keys)
    for row in target_primary_keys
]

deleted_primary_keys = set(target_primary_keys) - set(primary_keys)

print(f"Collected {len(list(set(primary_keys)))} primary keys in source between {start_date}")

print(f"Collected {len(list(set(target_primary_keys)))} primary keys in target")

print(f"Found {len(deleted_primary_keys)} primary keys in target but not in source (might be missing)")

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
schema = pk_df.schema
deleted_primary_keys_df = spark.createDataFrame(deleted_primary_keys, schema=schema)

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

# %%
