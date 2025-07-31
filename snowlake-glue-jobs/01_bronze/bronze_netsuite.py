# -*- coding: utf-8 -*-
# %%
import re
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
    folder="netsuite",
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
NETSUITE_RECORDS_LIMIT = 100_000

# %%

current_date = datetime.now().strftime("%Y-%m-%d")
current_timestamp = time.time()


def check_execution_time(datasource, current_timestamp, max_duration):
    """ Check if the execution time exceeded the max duration. If so, return a warning result. """

    max_duration_str = time.strftime("%Hh%Mm%Ss", time.gmtime(max_duration))
    if time.time() - current_timestamp > max_duration:
        logger.warning(f"{datasource.source_table_alias} - Cloture du pipeline jusqu'à la prochaine exécution car le temps d'exécution est écoulé ({max_duration_str}).")

        return {
            "table": datasource.source_table_alias,
            "status": "WARNING",
            "type": f"Cloture du pipeline jusqu'à la prochaine exécution car le temps d'exécution est écoulé {max_duration} secondes. ({max_duration_str} par défaut).",
            "message": f"{datasource.source_table_alias} Table exceded max duration {max_duration}seconds ({max_duration_str})",
            "traceback": traceback.format_exc(),
        }
    return None


def process_batch(batch_id: int, batch_index: int, data: list, datasource: datasources.NetsuiteSource, table_exists: bool = False):
    """ Process a batch of data and write it to Iceberg table.  """

    logger.info(f"Processing batch {batch_id} - subtask {batch_index} for datasource {datasource.source_table_alias} with data size {len(data)}")

    # Write to a tmp directory
    tmp_json_path = f"{s3_path}/tmp/{datasource.source_table_alias}.json"
    logger.info(f"Write a tmp file into s3 for further processing {datasource.source_table_alias}.json to {tmp_json_path}")
    etl_tools.write_json(data, tmp_json_path)

    # Prepare a path in case of failure
    failed_ingestions_staging_path = f"{s3_path}/failed_ingestions_staging/{datasource.source_table_alias}/{current_date}/{datasource.source_table_alias}_{current_timestamp}.json"

    try:

        # Read DataFrame
        input_df = spark.read.option("multiline", "true").json(tmp_json_path, schema=datasource.schema)

        # Add MetaData
        input_df = etl_tools.add_meta(input_df)

        # Coalesce DataFrame
        logger.info("Make sure we don't have small files : target is ~256MB per parquet fiLe")
        final_df = etl_tools.coalesce_dataframe(input_df, target_parquet_size_mb=PARQUET_SIZE_MB_MAX)

        # Warning if schema has changed
        if table_exists:
            target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")
            table_tools.check_schema_change(target_df, final_df)

        # Display schema
        if datasource.datetime_column:
            final_df = final_df.withColumn("_event", F.col(datasource.datetime_column))

        final_df.printSchema()

        # Write table to iceberg
        table_tools.write_iceberg(
            df=final_df,
            primary_keys=datasource.primary_keys,
            table_name=datasource.target_table_name,
            glue_context=gc,
            spark=spark,
            write_mode=datasource.write_mode,
            tableProperties={
                'write.spark.accept-any-schema': 'true'
            },
            options={
                "mergeSchema": "true"
            },
            orderedBy=datasource.primary_keys,
        )

        result = {
            "table": datasource.source_table_alias,
            "status": "SUCCESS",
            "batch_id": batch_id,
        }

        return result

    except Exception as e:
        logger.error(f"Batch {batch_id} - Transformation error on datasource '{datasource.source_table_alias}' : {e}")

        # Write failed ingestion json to a staging area in s3 for fallback and ingestion replay
        logger.info(f"{datasource.source_table_alias} - Writing failed ingestion's json file "
                    f"into a staging area in s3 for fallback and ingestion replay - {failed_ingestions_staging_path}")

        result = {
            "table": datasource.source_table_alias,
            "status": "ERROR",
            "type": f"Batch {batch_id} - Transformation error on datasource '{datasource.source_table_alias}'",
            "message": e,
            "traceback": traceback.format_exc(),
            "failed_ingestions_staging_path": failed_ingestions_staging_path,
            "data": data,
        }

        etl_tools.write_json(data, failed_ingestions_staging_path)

        return result


def get_initial_batch_query(datasource: datasources.NetsuiteSource, table_exists: bool, record_count: int, where_clause: str = None):

    logger.info(f"{datasource.source_table_alias} - Source API has {record_count} records")

    # Reset max date if table exists
    if table_exists:
        target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")

        target_df_total_count = target_df.count()

        target_df_active_count = datasource.get_active_count(target_df)

        last_date: datetime = datasource.get_last_date(target_df)
        last_ids: int = datasource.get_last_ids(target_df)

        logger.info(f"{datasource.source_table_alias} exists with {target_df_active_count} active records "
                    f"over {target_df_total_count} total records - last {datasource.primary_keys} = {last_ids} "
                    f"and last {datasource.datetime_column} = {last_date}")

        where_clause = where_clause if where_clause else datasource.get_where_clause(last_ids=last_ids, last_date=last_date)

        logger.info(f"Using where clause: {where_clause}")

        q = datasource.get_sql_query(
            where_clause=where_clause,
            order_by_clause=datasource.get_order_by_clause(),
        )
    else:
        q = datasource.get_sql_query(
            order_by_clause=datasource.get_order_by_clause(),
        )

    return q


def get_next_batch_query(batch_id, batch_data, datasource, target_df_current, active_count):
    """Update progress and check if more data is available."""

    last_ids = datasource.get_last_ids(target_df_current)
    last_date = datasource.get_last_date(target_df_current)

    # Check if we still have more data
    if all(d["hasMore"] is True for d in batch_data):

        logger.info(f"Batch id {batch_id} - {datasource.source_table_alias} - Found {active_count} active records"
                    f" with last id = {last_ids} and last modified date ="
                    f" {last_date} on target.. Trying to get more data...")

        return datasource.get_sql_query(
            where_clause=datasource.get_where_clause(last_ids=last_ids, last_date=last_date),
            order_by_clause=datasource.get_order_by_clause(),
        )

    # Stop iteration if no more data is available
    logger.info(f"Batch id {batch_id} - {datasource.source_table_alias} - "
                "Latest batch completed all records.. Stopping ingestion ! "
                f"Target table infos: active records count {active_count} - "
                f"last id = {last_ids} - last modified date = {last_date}")

    return None


def print_datasource_to_to_ingest(datasource: datasources.Datasource):
    d = asdict(datasource)

    # Convert 'schema' field if it's a StructType
    if "schema" in d and hasattr(d["schema"], "jsonValue"):
        d["schema"] = d["schema"].jsonValue()

    logger.info(f"Table(s) to ingest : {json.dumps(d, indent=2)}")


def run(datasource: datasources.NetsuiteSource, page_size=1000, max_duration=5400, max_workers=15, where_clause=None):
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

        # Get record count from the API
        record_count = datasource.get_record_count()

        # Get initial SQL query (first batch query or incremental batch query)
        q = get_initial_batch_query(
            datasource=datasource,
            table_exists=table_exists,
            record_count=record_count,
            where_clause=where_clause if where_clause else None,
        )

        # Fetch data from API
        pagination_strategy = snowlake_api_tools.OffsetPagination(
            page_size=page_size,
            limit_param="limit",
            offset_param="offset",
        )

        batch_id = 1
        has_more = True

        progressions = {}

        # Iterate over API results until all records are processed (ie: active_count == record_count)
        while has_more:

            # (Re)Connect
            connector = snowlake_api_tools.NetsuiteApiConnector()

            # Fetch pages lazily: This will yield results as they are fetched (return result without stoping execution)
            results_generator = connector.fetch_pages_lazy(
                url=datasource.url_path,
                pagination_strategy=pagination_strategy,
                json={"q": q},
                max_workers=max_workers,
                source_name=datasource.source_table_alias,
            )

            # Process data
            batch_index = 0

            for batch_data in results_generator:

                # Check if processing time exceeded
                result = check_execution_time(datasource, current_timestamp, max_duration)

                # If processing time exceeded, yield result and stop processing
                if result:
                    yield result
                    has_more = False
                    break  # Interruption du pipeline après un temps défini

                # If processing time is ok, continue processing
                logger.info(f"{datasource.source_table_alias} - Processing data batch {batch_id}")

                data = []

                # Extract records from batch data
                for d in batch_data:
                    data.extend(d["items"])

                # If data is not empty, process it, write to iceberg and yield result (SUCCESS or ERROR)
                if data:

                    result = process_batch(
                        batch_id=batch_id,
                        batch_index=batch_index,
                        data=data,
                        datasource=datasource,
                        table_exists=table_exists,
                    )

                    yield result

                else:
                    logger.warning(f"{datasource.source_table_alias} - Source data is empty ! ")

                    result = {
                        "table": datasource.source_table_alias,
                        "status": "WARNING",
                        "type": f"Batch {batch_id} - Emtpy data returned - source data is {data}",
                        "message": f"Batch {batch_id} - Subtask {batch_index} - Source data {data} is empty for {datasource.source_table_alias} !",
                        "traceback": traceback.format_exc(),
                        "data": data,
                    }

                    yield result

                # Get the current target table state to compute progression
                target_df_current = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")

                # Get the active count of records in the target table
                active_count = datasource.get_active_count(target_df_current)

                # Compute progression
                progression = active_count / record_count * 100
                progressions[batch_id] = progression

                logger.info(f"Batch id {batch_id} - {datasource.source_table_alias} - Progression"
                            f"  {progression:.2f}% : {active_count}/{record_count} records processed")

                # Get the next batch query based on the current table state and progressions
                q = get_next_batch_query(
                    batch_id=batch_id,
                    batch_data=batch_data,
                    datasource=datasource,
                    target_df_current=target_df_current,
                    active_count=active_count
                )

                # Update has_more based on the query result (if q is None, it means no more data is available)
                has_more = q is not None

                batch_id += 1
                batch_index += 1

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


if etl_tools.is_running_locally():
    choices = [d.source_table_alias for d in datasources.NetsuiteDataSources.get_all()]
    source_table_name = input(f"""Choose a Netsuite Datasource using it's source_table_alias (see datasources.py)
                            Choose between {choices}""")

    datasource = datasources.NetsuiteDataSources.get_one(source_table_name)

    max_duration = int(args.get("max-duration", 90*60))  # Temps maximum d'exécution en secondes (1h30 = 5400s)
    max_workers = int(args.get("max-workers", 15))
    page_size = int(args.get("page-size", datasource.page_size))
    where_clause = args.get("where_clause", None)
else:
    source_table_name = args.get(
        "table",
        re.search(r"netsuite_(.+)$", args["JOB_NAME"]).group(1)
    )
    datasource = datasources.NetsuiteDataSources.get_one(source_table_name)

    max_duration = int(args.get("max-duration", 120*60))  # Temps maximum d'exécution en secondes (2h = 7200s)
    max_workers = int(args.get("max-workers", 30))
    page_size = int(args.get("page-size", datasource.page_size))
    where_clause = args.get("where_clause", None)

print_datasource_to_to_ingest(datasource)

# %%

logger.info(f"Starting ingestion for datasource {datasource.source_table_alias} with max duration {max_duration} seconds ({time.strftime('%Hh%Mm%Ss', time.gmtime(max_duration))}) and max workers {max_workers} and page size {page_size}")
results = [
    result for result in
    run(
        datasource=datasource,
        max_duration=max_duration,
        max_workers=max_workers,
        page_size=page_size,
    )
]

etl_tools.display_results(results=results, datasources=[datasource])

# %%
