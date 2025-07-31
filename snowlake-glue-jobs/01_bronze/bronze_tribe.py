# -*- coding: utf-8 -*-
# %%
import time
import traceback
from datetime import datetime
import logging
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import etl_tools
import snowlake_api_tools
import table_tools
import datasources
import concurrent.futures


args = etl_tools.get_args()
conf = etl_tools.configure_iceberg(
    env=args["environment"],
    db="bronze",
    folder="tribe",
)

sc = SparkContext(conf=conf)
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session

job.init(args["JOB_NAME"], args)

env = args["environment"]
db = 'bronze'
desti = 'tribe'

logger = logging.getLogger(__name__)

s3_path = spark.conf.get("spark.environment.data_s3_path")
catalog = spark.conf.get("spark.environment.catalog_name")
database = spark.conf.get("spark.environment.database_name")
bucket_name = spark.conf.get("spark.environment.bucket_name")


PARQUET_SIZE_MB_MAX = 512

# %%

# Connect
connector = snowlake_api_tools.TribeApiConnector(
    env=env,
    token_s3_bucket=bucket_name,
    token_s3_key='bronze/tech/tribe_refresh_token',
)

# Check if token is valid or force refresh
connector = connector.check_token_or_reinitialize(
    url=datasources.TribeDataSources.OPPORTUNITY.url_path,
    params={"$top": 3}
)

# Mark job begining for debugging
current_date = datetime.now().strftime("%Y-%m-%d")
current_timestamp = time.time()

# %%


def run(datasource: datasources.TribeSource, page_size=None, dt_max=None):

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

            # # get max updated date from datasource's updated_date column
            # last_updated_date = target_df.select(F.max(F.col(datasource.updated_date).cast("timestamp"))).collect()[0][0]
            # logger.info(f"Table {datasource.source_table_alias} already exists, fetch data with last date = {last_updated_date}")

        #     datasource.dt_max = last_updated_date.strftime("%Y-%m-%dT%H:%M:%S")

        # else:
        #     logger.info(f"Table {datasource.source_table_alias} does not exist, fetch data from begining")
        #     datasource.dt_max = "2015-01-01T00:00:00"

        if dt_max:
            datasource.dt_max = dt_max

        # Fetch data from API
        pagination_strategy = snowlake_api_tools.OffsetPagination(
            page_size=page_size if page_size else datasource.page_size,
            limit_param="$top",
            offset_param="$skip"
        )

        data = connector.fetch_pages(
            url=datasource.url_path,
            pagination_strategy=pagination_strategy,
            max_workers=10,
            data_key="value",
            params=datasource.params,
            source_name=datasource.source_table_alias
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
                input_df = spark.read.option("multiline", "true").json(tmp_json_path, schema=datasource.schema)

                if datasource.explode_columns:
                    input_df = etl_tools.explode_array_columns(input_df, datasource.explode_columns)

                # Flatten and explode dataframe
                if datasource.flatten_columns:
                    input_df = etl_tools.flatten_struct_columns(input_df, flatten_cols=datasource.flatten_columns, explode_arrays=False)

                # Add MetaData
                input_df = etl_tools.add_meta(input_df)

                # Coalesce DataFrame
                logger.info("Make sure we don't have small files : target is ~256MB per parquet fiLe")
                final_df = etl_tools.coalesce_dataframe(input_df, target_parquet_size_mb=PARQUET_SIZE_MB_MAX)

                # Warning if schema has changed
                if table_exists:
                    table_tools.check_schema_change(target_df, final_df)

                # Drop duplicates as placeholder fix
                final_df = final_df.dropDuplicates(datasource.primary_keys)

                # Add metadata
                final_df = etl_tools.add_meta(final_df)

                # Print schema for debugging
                final_df.printSchema()

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
    api_data_sources = datasources.TribeDataSources.get(*args["tables"].split(","))
else:
    api_data_sources = datasources.TribeDataSources.get()

dt_max = args.get("dt_max", "2000-01-01T00:00:00")

logger.info(f"{len(api_data_sources)} Table(s) to ingest : {api_data_sources}")

# %%
with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    results = list(executor.map(lambda x: run(x, dt_max=dt_max), api_data_sources))

etl_tools.display_results(results, api_data_sources)

job.commit()

# %%
