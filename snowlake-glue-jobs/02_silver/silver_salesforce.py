# -*- coding: utf-8 -*-
# %%
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import table_tools
import etl_tools
import logging
import datasources
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyiceberg.catalog import load_catalog


# CONFIGURE ICEBGERG
args = etl_tools.get_args()
env = args["environment"]

if etl_tools.is_running_locally():
    choices = [d.source_table_alias for d in datasources.SalesforceDataSources.get_all()]
    source_table_name = input("""Choose a Salesforce Datasource using it's source_table_alias (see datasources.py)""")

    datasource = datasources.SalesforceDataSources.get_one(source_table_name)
else:
    source_table_name = args.get("table")
    datasource = datasources.SalesforceDataSources.get_one(source_table_name)


conf = etl_tools.configure_iceberg(
    env=args["environment"],
    db='silver',
    folder=f"salesforce/{datasource.target_table_name}"
)
sc = SparkContext(conf=conf)
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session

catalog = spark.conf.get("spark.environment.catalog_name")
database = spark.conf.get("spark.environment.database_name")
source_database = f"{env}_snowlake_salesforce"

logger = logging.getLogger(__name__)

# %%

logger.info(f"Retrieving table {datasource.source_table_name} from '{source_database}'...")

table_exists = etl_tools.check_table_exists(
        gc=gc,
        env=env,
        db='silver',
        table=datasource.target_table_name,
)

# %%


def merge_schema(
        datasource,
        source_df,
        target_df,
        catalog,
        source_database,
        target_database):

    """Merge the schema of the source DataFrame with the target DataFrame in Iceberg."""

    dropped_columns = set(target_df.columns) - set(source_df.columns)
    new_columns = set(source_df.columns) - set(target_df.columns)

    if dropped_columns:
        logger.warning(f"Some columns have been from source table {datasource.target_table_name} but are still present in target... {dropped_columns}")
        # TODO
        # Handle dropped columns if necessary, e.g., by removing them from the target table schema or by always setting them to null in target

    if new_columns:
        logger.warning(f"Some columns have been added to source table {datasource.target_table_name} but are not present in target... {new_columns}")
        pyiceberg_catalog = load_catalog(catalog)

        target = pyiceberg_catalog.load_table(f"{target_database}.{datasource.target_table_name}")
        source = pyiceberg_catalog.load_table(f"{source_database}.{datasource.source_table_name}")

        with target.update_schema() as update:
            update.union_by_name(source.schema())

# %%


def get_primary_keys(df: DataFrame, datasource: datasources.SalesforceDataSources):
    """Retrieve primary keys from the DataFrame based on the datasource configuration."""

    logger.info(f"Retrieving primary keys for table {datasource.target_table_name}...")
    primary_keys = (
        df
        .select(*datasource.primary_keys)
        .distinct()
        .collect()
    )

    return [
        row[datasource.primary_keys[0]] for row in primary_keys
    ]


# %%

df = spark.table(f"{catalog}.{source_database}.{datasource.source_table_name}")

if table_exists:

    target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")

    merge_schema(
        datasource=datasource,
        source_df=df,
        target_df=target_df,
        catalog=catalog,
        source_database=source_database,
        target_database=database
    )

    target_primary_keys = get_primary_keys(target_df, datasource)
    source_primary_keys = get_primary_keys(df, datasource)

    new_primary_keys = set(source_primary_keys) - set(target_primary_keys)
    deleted_primary_keys = set(target_primary_keys) - set(source_primary_keys)

    if deleted_primary_keys:

        logging.warning(f"Some primary keys have been deleted from source table {datasource.target_table_name} but are still present in target... {deleted_primary_keys}")

        logger.info(f"Collected {len(list(set(source_primary_keys)))} primary keys in source between")
        logger.info(f"Collected {len(list(set(target_primary_keys)))} primary keys in target")

        logger.info(f"Found {len(new_primary_keys)} new primary keys in source but not in target")
        logger.info(f"Found {len(deleted_primary_keys)} deleted primary keys in source but not in target")

        target_df = spark.table(f"{catalog}.{database}.{datasource.target_table_name}")

        table_tools.deactivate_deleted_records(
            target_df=target_df,
            datasource=datasource,
            spark=spark,
            deleted_primary_keys=deleted_primary_keys,
        )

        # TODO
        # Merge new records into target table
else:
    logging.info(f"Writing table {datasource.target_table_name} into '{database}'...")

    df = spark.table(f"{catalog}.{source_database}.{datasource.source_table_name}")
    df = df.withColumn("_is_deleted", F.lit(False))

    table_tools.write_iceberg(
        df=df,
        primary_keys=datasource.primary_keys,
        table_name=datasource.target_table_name,
        datetime_column=datasource.datetime_column,
        glue_context=gc,
        spark=spark,
        write_mode=datasource.write_mode,
        excluded_attribute_from_hash=["_is_deleted"],
        tableProperties={
            'write.spark.accept-any-schema': 'true'
        },
        options={
            "mergeSchema": "true"
        }
    )

# %%
