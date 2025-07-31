# -*- coding: utf-8 -*-
# %%
import time
import json
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
from pyspark.sql import functions as F
from dataclasses import asdict


args = etl_tools.get_args()
conf = etl_tools.configure_iceberg(
    env=args["environment"],
    db="bronze",
    folder="afas",
)

sc = SparkContext(conf=conf)
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session

job.init(args["JOB_NAME"], args)

logger = logging.getLogger(__name__)

s3_path = spark.conf.get("spark.environment.data_s3_path")
catalog = spark.conf.get("spark.environment.catalog_name")
database = spark.conf.get("spark.environment.database_name")
bucket = spark.conf.get("spark.environment.bucket_name")

PARQUET_SIZE_MB_MAX = 512

# %%
# Connect
connector = snowlake_api_tools.AfasApiConnector()

current_date = datetime.now().strftime("%Y-%m-%d")
current_timestamp = time.time()


def run(datasource: datasources.AfasSource, filter_params=None, min_year="2012", page_size=None, max_duration=5400, max_workers=5000):

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
        if table_exists and datasource.datetime_column:
            target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")

            last_date: datetime = target_df.select(
                F.max(F.to_timestamp(datasource.datetime_column))
            ).collect()[0][0]

            last_date_str = last_date.strftime('%Y-%m-%dT%H:%M:%S')

            logger.info(f"{datasource.source_table_alias} exists with last {datasource.datetime_column} = {last_date_str}")

            params = {
                "filterfieldids": datasource.datetime_column,
                "filtervalues": last_date_str,
                "orderbyfieldids": ",".join(datasource.order_columns),
                "operatortypes": "2"
            }
        else:
            params = {
                "filterfieldids": "Year",
                "filtervalues": min_year,
                "orderbyfieldids": ",".join(datasource.order_columns),
                "operatortypes": "2"
            }

        if filter_params:
            params = filter_params

        # Fetch data from API
        pagination_strategy = snowlake_api_tools.OffsetPagination(
            page_size=page_size,
            limit_param="take",
            offset_param="skip"
        )

        results_generator = connector.fetch_pages_lazy(
            url=datasource.url_path,
            pagination_strategy=pagination_strategy,
            data_key="rows",
            params=params,
            max_workers=max_workers,
            source_name=datasource.source_table_alias,
        )

        batch_id = 1

        for data in results_generator:

            if time.time() - current_timestamp > max_duration:
                logger.warning(f"{datasource.source_table_alias} - Cloture du pipeline jusqu'à la prochaine exécution car le temps d'exécution est écoulé (1h30).")

                yield {
                    "table": datasource.source_table_alias,
                    "status": "WARNING",
                    "type": f"Cloture du pipeline jusqu'à la prochaine exécution car le temps d'exécution est écoulé {max_duration} secondes. (1h30 par défaut).",
                    "message": f"{datasource.source_table_alias} Table exceded max duration {max_duration}seconds",
                    "traceback": traceback.format_exc(),
                }

                break  # Interruption du pipeline après un temps défini

            logger.info(f"{datasource.source_table_alias} - Processing data batch {batch_id}")

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
                        table_tools.check_schema_change(target_df, final_df)

                    # Display schema
                    final_df.printSchema()

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
                        orderedBy=datasource.order_columns,
                        datetime_column=datasource.datetime_column
                    )

                    yield {
                        "table": datasource.source_table_alias,
                        "status": "SUCCESS",
                        "batch_id": batch_id,
                        "df": final_df,
                    }

                    batch_id += 1

                except Exception as e:
                    logger.error(f"Transformation error on datasource '{datasource.source_table_alias}' : {e}")

                    # Write failed ingestion json to a staging area in s3 for fallback and ingestion replay
                    logger.info(f"{datasource.source_table_alias} - Writing failed ingestion's json file "
                                f"into a staging area in s3 for fallback and ingestion replay - {failed_ingestions_staging_path}")

                    etl_tools.write_json(data, failed_ingestions_staging_path)

                    yield {
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

                yield {
                    "table": datasource.source_table_alias,
                    "status": "WARNING",
                    "type": f"Emtpy data returned - source data is {data}",
                    "message": f"Source data {data} is empty for {datasource.source_table_alias} !",
                    "traceback": traceback.format_exc(),
                    "data": data,
                }

    except Exception as e:

        logger.error(f"API Request error on datasource '{datasource.source_table_alias}' : {e}")

        yield {
            "table": datasource.source_table_alias,
            "status": "ERROR",
            "type": f"API Request error on datasource '{datasource.source_table_alias}'",
            "message": e,
            "traceback": traceback.format_exc(),
        }


# %%

datasource = datasources.AfasDataSources.PROFIT_FINANCIELE_MUTATIES
if etl_tools.is_running_locally():
    min_year = args.get("min-year", "2023")
    max_duration = int(args.get("max-duration", 90*60))  # Temps maximum d'exécution en secondes (1h30 = 5400s)
    max_workers = int(args.get("max-workers", 15))
    page_size = int(args.get("page-size", datasource.page_size))
else:
    min_year = args.get("min-year", "2023")
    max_duration = int(args.get("max-duration", 120*60))  # Temps maximum d'exécution en secondes (1h30 = 5400s)
    max_workers = int(args.get("max-workers", 20))
    page_size = int(args.get("page-size", datasource.page_size * 1.5))

logger.info(f"Table(s) to ingest : {json.dumps(asdict(datasource), indent=2)}")

# %%

results = [
    result for result in
    run(
        datasource=datasource,
        min_year=min_year,
        max_duration=max_duration,
        max_workers=max_workers,
        page_size=page_size,
        # filter_params = {
        #         "filterfieldids": "Year,Year",
        #         "filtervalues": "2023,2024",
        #         "orderbyfieldids": ",".join(datasource.order_columns),
        #         "operatortypes": "2,5"
        # }
    )
]

etl_tools.display_results(results=results, datasources=[datasource])
