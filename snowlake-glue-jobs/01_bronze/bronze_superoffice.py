# -*- coding: utf-8 -*-

# %%
import logging
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from datetime import datetime
import time
import traceback
import etl_tools
import snowlake_api_tools
import table_tools
import datasources
from pyspark.sql import functions as F
import concurrent.futures

args = etl_tools.get_args()
conf = etl_tools.configure_iceberg(
    env=args["environment"],
    db="bronze",
    folder="superoffice",
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

current_date = datetime.now().strftime("%Y-%m-%d")
current_timestamp = time.time()


def check_schema_change(target_df, final_df):

    target_schema_diff = {c for c in set(target_df.schema) - set(final_df.schema) if not c.name.startswith("_")}
    if target_schema_diff:
        logger.warning(f"Some columns have been changed or removed {target_schema_diff}")

    source_schema_diff = {c for c in set(final_df.schema) - set(target_df.schema) if not c.name.startswith("_")}
    if source_schema_diff:
        logger.warning(f"Some columns have been changed or added {source_schema_diff}")


def fetch_pages(datasource, page_size, filters=None, max_workers=10):

    data = []
    current_page = 0

    select = f"&$select={','.join(datasource.columns)}" if datasource.columns else ""

    # Keep the thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

        # Loop until no more data is returned
        while True:

            futures = []

            # Fetch 'max_workers (default to 10)' pages concurrently in each loop iteration
            for i in range(max_workers):
                skip = (current_page + i) * page_size
                url = f"{datasource.url_path}?{f'$filter={filters}&' if filters else ''}$top={page_size}&$skip={skip}{select}"
                futures.append(executor.submit(connector.get, url))

            page_data_group = [future.result() for future in concurrent.futures.as_completed(futures)]

            # Extend data with fetched values:
            for page_data in page_data_group:
                values = page_data.get("value", [])
                data.extend(values)

            # Check if any of the fetched pages are empty
            if any(len(page_data.get("value", [])) == 0 for page_data in page_data_group):
                break  # Exit the loop if any page is empty

            # increment page number for the next batch of pages
            current_page += max_workers

    logger.info(f'{datasource.source_table_alias} - Fetched a total of {len(data)} records - Filters : {filters}')

    return data


def run(datasource: datasources.SuperofficeSource, page_size: int = None, start_from: str = None, max_workers=10):

    try:

        # Ingest
        logger.info(f"Table to ingest : {datasource}")

        # Check if table exists
        table_exists = etl_tools.check_table_exists(
            gc=gc,
            env=args["environment"],
            db="bronze",
            table=datasource.target_table_name,
        )

        # Reset max date if table exists
        if table_exists:
            target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")

            # get max updated date from datasource's updated_date column
            last_updated_date = target_df.select(F.max(F.col(datasource.updated_date).cast("date"))).collect()[0][0]
            filters = f"{datasource.updated_date} after '{last_updated_date}'"

            logger.info(f"Table {datasource.source_table_alias} already exists, fetch data with filters {filters}")

        else:
            filters = None
            logger.info(f"Table {datasource.source_table_alias} does not exist, fetch data from begining")

        filters = f"{datasource.updated_date} after '{start_from}'" if start_from else filters

        # Fetch data from API
        data = fetch_pages(
            datasource=datasource,
            page_size=page_size if page_size else datasource.page_size,
            filters=filters,
            max_workers=max_workers,
        )

        if data:

            # Write to a tmp directory
            tmp_json_path = f"{s3_path}/tmp/{datasource.source_table_alias}.json"
            logger.info(f"Write a tmp file into s3 for further processing {datasource.source_table_alias}.json to {tmp_json_path}")
            etl_tools.write_json(data, tmp_json_path)

            # Prepare a path in case of failure
            failed_ingestions_staging_path = f"{s3_path}/failed_ingestions_staging/{datasource.source_table_alias}/{current_date}/{datasource.source_table_alias}_{current_timestamp}.json"

            try:

                # Read DataFrame
                input_df = spark.read.option("multiline", "true").json(tmp_json_path)

                # Add MetaData
                input_df = etl_tools.add_meta(input_df)

                # Coalesce DataFrame
                logger.info("Make sure we don't have small files : target is ~256MB per parquet fiLe")
                final_df = etl_tools.coalesce_dataframe(input_df, target_parquet_size_mb=PARQUET_SIZE_MB_MAX)

                # Warning if schema has changed
                if table_exists:
                    check_schema_change(target_df, final_df)

                # Display schema
                final_df.printSchema()

                final_df.show()

                # Write table to iceberg
                table_tools.write_iceberg(
                    df=final_df,
                    primary_keys=datasource.primary_keys,
                    table_name=datasource.target_table_name,
                    glue_context=gc,
                    spark=spark,
                    write_mode=datasource.write_mode,  # Defaults to SCD2
                    tableProperties={
                        'write.spark.accept-any-schema': 'true'
                    },
                    options={
                        "mergeSchema": "true"
                    },
                )

                return {
                    "table": datasource.source_table_alias,
                    "status": "SUCCESS"
                }

            except Exception as e:
                logger.error(f"Transformation error on datasource '{datasource.source_table_alias}' : {e}")

                # Write failed ingestion json to a staging area in s3 for fallback and ingestion replay
                logger.info(f"{datasource.source_table_alias} - Writing failed ingestion's json file "
                            f"into a staging area in s3 for fallback and ingestion replay - {failed_ingestions_staging_path}")

                etl_tools.write_json(data, failed_ingestions_staging_path)

                return {
                    "table": datasource.source_table_alias,
                    "status": "ERROR",
                    "type": f"Transformation error on datasource '{datasource.source_table_alias}'",
                    "message": e,
                    "traceback": traceback.format_exc(),
                    "failed_ingestions_staging_path": failed_ingestions_staging_path,
                    "data": data,
                }

        else:
            logger.warning(f"{datasource.source_table_alias} - Source data is empty ! ")

            return {
                "table": datasource.source_table_alias,
                "status": "WARNING",
                "type": f"Emtpy data returned - source data is {data}",
                "message": f"Source data {data} is empty for {datasource.source_table_alias} !",
                "traceback": traceback.format_exc(),
                "data": data,
            }

    except Exception as e:

        logger.error(f"API Request error on datasource '{datasource.source_table_alias}' : {e}")

        return {
            "table": datasource.source_table_alias,
            "status": "ERROR",
            "type": f"API Request error on datasource '{datasource.source_table_alias}'",
            "message": e,
            "traceback": traceback.format_exc(),
        }


# %%
if "tables" in args:
    api_data_sources = datasources.SuperofficeDataSources.get(*args["tables"].split(","))
else:
    api_data_sources = datasources.SuperofficeDataSources.get()

max_workers = args.get("max_workers", 10)
start_from = args.get("start_from")

logger.info(f"{len(api_data_sources)} Table(s) to ingest : {api_data_sources}")
# %%
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(lambda x: run(x, start_from=start_from, ), api_data_sources))

etl_tools.display_results(results, api_data_sources)

job.commit()

# %%
