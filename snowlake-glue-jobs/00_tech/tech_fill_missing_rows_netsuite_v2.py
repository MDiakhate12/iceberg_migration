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
import concurrent.futures
import time
import traceback


# JOB CONTEXT SETUP
args = etl_tools.get_args(env="prod")
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


def check_execution_time(datasource, current_timestamp, max_duration):
    """ Check if the execution time exceeded the max duration. If so, return a warning result. """

    max_duration_str = time.strftime("%Hh%Mm%Ss", time.gmtime(max_duration))
    if time.time() - current_timestamp > max_duration:
        print(f"{datasource.source_table_alias} - Cloture du pipeline jusqu'à la prochaine exécution car le temps d'exécution est écoulé ({max_duration_str}).")

        return {
            "table": datasource.source_table_alias,
            "status": "WARNING",
            "type": f"Cloture du pipeline jusqu'à la prochaine exécution car le temps d'exécution est écoulé {max_duration} secondes. ({max_duration_str} par défaut).",
            "message": f"{datasource.source_table_alias} Table exceded max duration {max_duration}seconds ({max_duration_str})",
            "traceback": traceback.format_exc(),
        }
    return None


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


def build_condition(id_cols, last_ids, id_operator):
    conditions = []
    for j in range(len(id_cols)):
        parts = [
            f"TO_NUMBER({id_cols[i]}) = {last_ids[i]}" for i in range(j)
        ]

        parts.append(f"TO_NUMBER({id_cols[j]}) {id_operator} {last_ids[j]}")

        conditions.append(f"({' AND '.join(parts)})")

    return """
        OR """.join(conditions)


def build_query(datasource, start_date, end_date, date_column, additional_conditions=""):
    return f"""
    SELECT DISTINCT {",".join(datasource.primary_keys)}
    FROM {datasource.remote_table_name}
    WHERE
        TO_DATE({date_column}) >= '{start_date}' AND
        TO_DATE({date_column}) <= '{end_date}'
        {additional_conditions}
    ORDER BY {", ".join(f"TO_NUMBER({pk})" for pk in datasource.primary_keys)}
    """


def get_records_from_primary_keys(datasource, primary_keys_list):

    """ Fetch records from the source table based on the provided primary keys. """

    number_of_primary_keys = len(primary_keys_list)

    print(f"Fetching {datasource.source_table_alias} source data for {number_of_primary_keys} missing primary keys...")

    is_running_locally = etl_tools.is_running_locally()

    if is_running_locally:
        max_workers = min(30, number_of_primary_keys)
    else:
        max_workers = min(100, number_of_primary_keys)

    print(f"Using {max_workers} workers for fetching data.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

        futures = {}

        id_prefix = f"{datasource.query_table_alias}." if datasource.query_table_alias else ""

        for pk_tuple in primary_keys_list:
            where_clause = f"""WHERE {' AND '.join(f"{id_prefix}{pk} = '{pk_value}'" for pk, pk_value in zip(datasource.primary_keys, pk_tuple))}"""
            print(f"Preparing SQL query for primary key {pk_tuple}: {where_clause}")

            q = datasource.get_sql_query(
                where_clause=where_clause,
            )

            task = executor.submit(
                connector.get,
                url=datasource.url_path,
                json={"q": q},
            )

            futures[task] = pk_tuple

        results = []
        for future in concurrent.futures.as_completed(futures):
            pk_tuple = futures[future]
            try:
                result = future.result()
                results.extend(result["items"])
            except Exception as e:
                print(f"Error fetching data for primary key {pk_tuple}: {e}")

        print(f"Fetched {len(results)} {datasource.source_table_alias} records for missing primary keys.")

        return results


def create_dataframe_from_records(datasource, data):
    """ Create a Spark DataFrame from the fetched records. """
    print(f"Creating DataFrame for missing primary keys in {datasource.source_table_alias}.")

    s3_path = spark.conf.get("spark.environment.data_s3_path")

    # Write to a tmp directory
    tmp_json_path = f"{s3_path}/tmp/{datasource.source_table_alias}.json"
    print(f"Write a tmp file into s3 for further processing {datasource.source_table_alias}.json to {tmp_json_path}")
    etl_tools.write_json(data, tmp_json_path)

    # Read DataFrame
    missing_rows_df = spark.read.option("multiline", "true").json(tmp_json_path, schema=datasource.schema)

    # Add MetaData
    missing_rows_df = etl_tools.add_meta(missing_rows_df)

    # Warning if schema has changed
    target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")
    table_tools.check_schema_change(target_df, missing_rows_df)

    # Display schema
    if datasource.datetime_column:
        missing_rows_df = missing_rows_df.withColumn("_event", F.col(datasource.datetime_column))

    return missing_rows_df


def run(datasource, start_date, end_date, max_duration, max_workers=25, base_query_conditions=""):
    """ Main function to run the data processing pipeline. """

    base_query = build_query(
        datasource=datasource,
        start_date=start_date,
        end_date=end_date,
        date_column=datasource.datetime_column,
        additional_conditions=base_query_conditions
    )

    print(f"Base query: {base_query}")

    has_more = True
    data = []
    metadata = []
    primary_keys = []
    q = base_query
    current_timestamp = time.time()
    batch_id = 1

    while has_more:

        # Check if the execution time exceeded the max duration

        result = check_execution_time(datasource, current_timestamp, max_duration)

        # If processing time exceeded, yield result and stop processing
        if result:
            print(result)
            has_more = False
            break  # Interruption du pipeline après un temps défini

        print(f"Fetching batch {batch_id}...")
        result_generator = connector.fetch_pages_lazy(
            url=datasource.url_path,
            pagination_strategy=pagination_strategy,
            json={"q": q},
            max_workers=max_workers,
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

            print(f"Batch {batch_id} - Collected {len(list(set(primary_keys)))} primary keys in source between {start_date} and {end_date}")

            print(f"Batch {batch_id} - Found {len(set(primary_keys) - set(target_primary_keys))} primary keys in source but not in target (might be missing)")

            missing_primary_keys = set(primary_keys) - set(target_primary_keys)

            if missing_primary_keys:

                results = get_records_from_primary_keys(
                    datasource=datasource,
                    primary_keys_list=missing_primary_keys
                )

            else:
                print(f"No missing primary keys to fetch data for in batch {batch_id}.")
                results = []

            if results:

                missing_rows_df = create_dataframe_from_records(
                    datasource=datasource,
                    data=results
                )

                missing_rows_df.show()

                print(f"Writing {len(results)} missing rows to Iceberg table {datasource.target_table_name}.")

                table_tools.write_iceberg(
                    df=missing_rows_df,
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
                )

            batch_id += 1

            if len(primary_keys) >= record_count or any(d["hasMore"] is False for d in batch_data):
                has_more = False
                break

            last_ids = max(primary_keys)

            print(f"Last ID in batch {batch_id}: {last_ids}")

            additional_conditions = build_condition(
                id_cols=datasource.primary_keys,
                last_ids=last_ids,
                id_operator=">="
            )

            # Prepare next query with additional conditions for pagination
            q = build_query(
                datasource=datasource,
                start_date=start_date,
                end_date=end_date,
                date_column=datasource.datetime_column,
                additional_conditions=f" AND ({additional_conditions})"
            )
            print(f"Next query for batch {batch_id}: {q}")


# %%


# Retrieve dates from args or calculate default values
today = datetime.today()
first_day_of_last_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
start_date = first_day_of_last_month.strftime("%d/%m/%Y")
end_date = today.strftime("%d/%m/%Y")

print(f"Default start date: {start_date}, end date: {end_date}")

# start_date = args.get("start_date", start_date)
# end_date = args.get("end_date", end_date)
start_date = "01/01/2024"
end_date = "31/12/2025"
table = args.get("table", "transaction")
datasource = datasources.NetsuiteDataSources.get_one(table)


record_count = get_record_count(
    datasource=datasource,
    start_date=start_date,
    end_date=end_date,
    date_column=datasource.datetime_column
)
print(f"Total records to process: {record_count}")

connector = snowlake_api_tools.NetsuiteApiConnector()
pagination_strategy = snowlake_api_tools.OffsetPagination(
    page_size=datasource.page_size,
    limit_param="limit",
    offset_param="offset",
)

# %%
print(f"Collecting target primary keys for {datasource.source_table_alias}.")
target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")

target_primary_keys = (
    target_df
    .select(*datasource.primary_keys)
    .distinct()
    .collect()
)

target_primary_keys = [
    tuple(int(row[col]) for col in datasource.primary_keys)
    for row in target_primary_keys
]
print(f"Collected {len(list(set(target_primary_keys)))} primary keys in target")

# %%
max_duration = int(args.get("max-duration", 3600 * 3))  # Default to 3 hour if not set
max_workers = int(args.get("max-workers", 50))  # Default to 50 workers if not set
run(
    datasource=datasource,
    start_date=start_date,
    end_date=end_date,
    max_duration=max_duration,
    max_workers=max_workers,
)
# %%
