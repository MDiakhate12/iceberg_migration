# -*- coding: utf-8 -*-
# %%
import time
import traceback
from datetime import datetime
import logging
from typing import List
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import etl_tools  # alway import elt_tools before other modules that uses the logger
import snowlake_api_tools
import table_tools
import datasources
from concurrent.futures import ThreadPoolExecutor


args = etl_tools.get_args()  # En dev mettre etl_tools.get_args(tables="companies,contracts...") pour renseigner les tables que vous voulez tester
conf = etl_tools.configure_iceberg(
    env=args["environment"],
    db="bronze",
    folder="webkua",
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

PARQUET_SIZE_MB_MAX = 512

# %%

# Connect
connector = snowlake_api_tools.FocusApiConnector()

current_date = datetime.now().strftime("%Y-%m-%d")
current_timestamp = time.time()

# %%


def run(datasource: datasources.WebkuaSource):
    try:
        # Fetch data
        data = connector.get(datasource.url_path)

        # Prepare a path in case of failure
        failed_ingestions_staging_path = f"{s3_path}/failed_ingestions_staging/{datasource.source_table_alias}/{current_date}/{datasource.source_table_alias}_{current_timestamp}.json"

        # Write to a tmp directory
        tmp_json_path = f"{s3_path}/tmp/{datasource.source_table_alias}.json"
        logger.info(f"Write a tmp file into s3 for further processing {datasource.source_table_alias}.json to {tmp_json_path}")
        etl_tools.write_json(data, tmp_json_path)

        # Transform / Actions
        try:
            logger.info(f"Tranform {datasource.source_table_alias} JSON to Dataframe")

            # Read in spark
            logger.info(f"Read json for {tmp_json_path} from tmp file in s3")
            df = spark.read.option("multiline", "true").json(f"{tmp_json_path}")

            logger.info(f"{datasource.source_table_alias} - Make sure we don't have small files : target is ~256MB per parquet fiLe")
            df = etl_tools.coalesce_dataframe(df, target_parquet_size_mb=PARQUET_SIZE_MB_MAX)  # 256MB par partition

            # Flatten Dataframe
            df = etl_tools.flatten_struct_columns(df, explode_arrays=False)

            # Add metadata
            df = etl_tools.add_meta(df)

            df.printSchema()

            table_tools.write_iceberg(
                df=df,
                primary_keys=datasource.primary_keys,
                table_name=datasource.target_table_name,
                glue_context=gc,
                spark=spark,
                write_mode=table_tools.WriteMode.SCD2 if datasource.primary_keys else table_tools.WriteMode.OVERWRITE
            )

            # Retour / Sortie
            return {
                "table": datasource.source_table_alias,
                "status": "SUCCESS",
            }

        except (Exception, ValueError) as e:
            logger.error(f"Transformation error on datasource '{datasource.source_table_alias}' : {e}")

            # Write failed ingestion json to a staging area in s3 for fallback and ingestion replay
            logger.info(f"{datasource.source_table_alias} - Writing failed ingestion's json file "
                        f"into a staging area in s3 for fallback and ingestion replay - {failed_ingestions_staging_path}")

            etl_tools.write_json(data, failed_ingestions_staging_path)

            return {
                "table": datasource.source_table_alias,
                "status": "ERROR",
                "type": "Transformation error",
                "message": e,
                "traceback": traceback.format_exc(),
                "failed_ingestions_staging_path": failed_ingestions_staging_path,
            }

    except Exception as e:
        logger.error(f"API Request error on datasource '{datasource.source_table_alias}' : {e}")

        # Write failed ingestion json to a staging area in s3 for fallback and ingestion replay
        logger.info(f"{datasource.source_table_alias} - Writing failed ingestion's json file "
                    f"into a staging area in s3 for fallback and ingestion replay - {failed_ingestions_staging_path}")

        etl_tools.write_json(data, failed_ingestions_staging_path)

        return {
                "table": datasource.source_table_alias,
                "status": "ERROR",
                "type": "API Request error",
                "message": e,
                "traceback": traceback.format_exc(),
                "failed_ingestions_staging_path": failed_ingestions_staging_path,
        }


# List source tables to ingest (WARNING ! default to all tables if argument --tables is not specified in webkua's terraform workflow)
api_data_sources: List[datasources.APISource] = []

if "tables" in args:
    api_data_sources = datasources.WebkuaDataSources.get(args["tables"].split(","))
else:
    api_data_sources = datasources.WebkuaDataSources.get()

logger.info(f"Tables to ingest : {api_data_sources}")


# Ingest
with ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(run, api_data_sources))

etl_tools.display_results(results, api_data_sources)

job.commit()

# %%
