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
args = etl_tools.get_args(env='prod')
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
    q = f"""
    SELECT
        COUNT(*)
    FROM {datasource.remote_table_name}
    WHERE
        TO_DATE({date_column}) >= '{start_date}' AND
        TO_DATE({date_column}) <= '{end_date}'
    """

    print(f"Record count query: {q}")

    record_count_data = snowlake_api_tools.NetsuiteApiConnector().get(
        url=datasource.url_path,
        json={"q": q},
    )

    print(record_count_data)

    result = record_count_data.get("items", [])

    if not result:
        print(f"No records found for the query: {q}")
        return 0

    return int(result[0]['expr1'])


def collect_primary_keys(datasource, start_date=None, end_date=None, date_column=None):

    def build_query(datasource, start_date, end_date, date_column, additional_conditions=""):
        return f"""
        SELECT DISTINCT {",".join(datasource.primary_keys)}
        FROM {datasource.source_table_alias}
        WHERE
            TO_DATE({date_column}) >= '{start_date}' AND
            TO_DATE({date_column}) <= '{end_date}'
            {additional_conditions}
        ORDER BY {", ".join(f"TO_NUMBER({pk})" for pk in datasource.primary_keys)}
        """

    base_query = build_query(
        datasource=datasource,
        start_date=start_date,
        end_date=end_date,
        date_column=date_column
    )

    print(f"Base query: {base_query}")

    connector = snowlake_api_tools.NetsuiteApiConnector()
    pagination_strategy = snowlake_api_tools.OffsetPagination(
        page_size=datasource.page_size,
        limit_param="limit",
        offset_param="offset",
    )

    record_count = get_record_count(datasource, start_date, end_date, date_column)

    if record_count == 0:
        print(f"No records found for the date range {start_date} to {end_date}.")
        return []

    else:
        print(f"Total records to process: {record_count}")
        has_more = True
        data = []
        metadata = []
        primary_keys = []
        batch_id = 1
        q = base_query
        print(q)

        while has_more:
            print(f"Fetching batch {batch_id}...")
            result_generator = connector.fetch_pages_lazy(
                url=datasource.url_path,
                pagination_strategy=pagination_strategy,
                json={"q": q},
                max_workers=25,
                source_name=datasource.source_table_alias,
            )

            for batch_data in result_generator:

                # Extract records from batch data
                for d in batch_data:
                    data.extend(d["items"])
                    metadata.append({k: v for k, v in d.items() if k != "items"})

                for d in data:
                    primary_key = tuple(int(d[pk]) for pk in datasource.primary_keys)
                    primary_keys.append(primary_key)

                primary_keys = list(set(primary_keys))  # Ensure uniqueness

                print(f"Fetched {len(primary_keys)}/{record_count} ids for batch {batch_id}...")
                print(f"Metadata for batch {batch_id}: {metadata}")

                if len(primary_keys) >= record_count or any(d["hasMore"] is False for d in batch_data):
                    has_more = False
                    break

                last_id = max(primary_keys)

                print(f"Last ID in batch {batch_id}: {last_id}")

                batch_id += 1

                # Prepare next query with additional conditions for pagination
                q = build_query(
                    datasource,
                    start_date,
                    end_date,
                    date_column,
                    additional_conditions=f"AND ({' OR '.join(f'TO_NUMBER({pk}) > {last_id[i]}' for i, pk in enumerate(datasource.primary_keys))})"
                )
                print(f"Next query for batch {batch_id}: {q}")

        return primary_keys


# %%

# Retrieve dates from args or calculate default values

if etl_tools.is_running_locally():
    start_date = input("Enter start date (dd/mm/yyyy) or leave empty for default: ")
    end_date = input("Enter end date (dd/mm/yyyy) or leave empty for default: ")
    table = input("Enter table name: ")
else:
    today = datetime.today()
    first_day_of_last_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    start_date = first_day_of_last_month.strftime("%d/%m/%Y")
    end_date = today.strftime("%d/%m/%Y")


print(f"Default start date: {start_date}, end date: {end_date}")

start_date = args.get("start_date", start_date)
end_date = args.get("end_date", end_date)

table = args.get("table", table)
datasource = datasources.NetsuiteDataSources.get_one(table)
# %%
print(f"Using start date: {start_date}, end date: {end_date} for table: {table}")

print(f"Collecting source primary keys for {datasource.source_table_alias} from {start_date} to {end_date}")
primary_keys = collect_primary_keys(
    datasource=datasource,
    start_date=start_date,
    end_date=end_date,
    date_column=datasource.datetime_column,
)

if not primary_keys:
    print(f"No primary keys found for the date range {start_date} to {end_date}.")
    job.commit()
    exit(0)

# %%
print(f"Collecting target primary keys for {datasource.source_table_alias}.")
target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")

target_primary_keys = (
    target_df
    .where(F.to_date(datasource.datetime_column, 'dd/MM/yyyy') >= F.to_date(F.lit(start_date), 'dd/MM/yyyy'))
    .where(F.to_date(datasource.datetime_column, 'dd/MM/yyyy') <= F.to_date(F.lit(end_date), 'dd/MM/yyyy'))
    .select(*datasource.primary_keys)
    .distinct()
    .collect()
)

target_primary_keys = [
    tuple(int(row[col]) for col in datasource.primary_keys)
    for row in target_primary_keys
]

deleted_primary_keys = set(target_primary_keys) - set(primary_keys)

print(f"Collected {len(list(set(primary_keys)))} primary keys in source between {start_date} and {end_date}")

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
# %%
