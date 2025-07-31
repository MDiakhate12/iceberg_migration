# -*- coding: utf-8 -*-
# %%
import traceback
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from etl_tools import (write_s3, add_meta,
                       check_table_exists, display_results,
                       get_args, configure_iceberg, is_running_locally)

from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.functions import col, struct
from pyspark.sql.types import StructType


logger = logging.getLogger(__name__)

# JOB CONTEXT SETUP

env = "prod"
db = "external"
# %%
args = get_args()

# %%
conf = configure_iceberg(
    env=args["environment"],
    db=db,
    folder="jman",
)

if is_running_locally():
    logger.info("Running locally.. Setting memory to 28g.")
    conf.set("spark.driver.memory", "28g").set("spark.executor.memory", "28g")

sc = SparkContext(conf=conf)
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session

job.init(args["JOB_NAME"], args)

logger = logging.getLogger(__name__)

s3_path = spark.conf.get("spark.environment.data_s3_path")
catalog = spark.conf.get("spark.environment.catalog_name")
database = spark.conf.get("spark.environment.database_name")


def clean_column_names(df):
    """
    Recursively renames all columns in a PySpark DataFrame, including struct subfields.
    - Replaces '.' with '_'
    - Removes '@' completely
    - Replaces ':' with '_'
    - Removes backticks (`)

    Parameters:
        df (DataFrame): The input Spark DataFrame.

    Returns:
        DataFrame: A DataFrame with cleaned column names.
    """

    def clean_name(name):
        """Apply renaming rules to column names."""
        return name.replace("@", "").replace(".", "_").replace(":", "_").replace("`", "")

    def rename_struct_fields(struct_col, struct_schema):
        """Recursively rename fields inside struct columns."""
        new_fields = [
            col(f"{struct_col}.`{field.name}`").alias(clean_name(field.name))
            if not isinstance(field.dataType, StructType) else
            rename_struct_fields(f"{struct_col}.`{field.name}`", field.dataType)  # Recursive for nested structs
            for field in struct_schema.fields
        ]
        return struct(*new_fields).alias(clean_name(struct_col))

    # Process all columns in the DataFrame
    new_columns = [
        rename_struct_fields(field.name, field.dataType) if isinstance(field.dataType, StructType)
        else col(f"`{field.name}`").alias(clean_name(field.name))
        for field in df.schema.fields
    ]

    # Return DataFrame with renamed columns
    return df.select(*new_columns)


# %%
table_requirements = [
    'netsuite_customer',
    'netsuite_entity',
    'netsuite_item',
    'netsuite_subscription',
    'netsuite_subscriptionline',
    'netsuite_subsidiary',
    'netsuite_transaction',
    'netsuite_transactionline',
    "netsuite_revenueelement",
    "netsuite_revenueplan",
    "netsuite_consolidatedexchangerate",
    "netsuite_revenueplanplannedrevenue",

    'ref_client',
    'ref_groupe_client',
    'ref_secteur',
    'ref_type_client',

    'scream_client_to_produit',
    'scream_societe',
    'scream_produit',
    'scream_produit_to_produit',
    "scream_societe",
    "scream_produit",
    "scream_entite_to_societe",
    "scream_origine",

    'sogetask_affaire',
    'sogetask_bon_commande',
    'sogetask_devis',
    'sogetask_devise',
    'sogetask_produit_devis',
    'sogetask_tache',

    'superoffice_contact',
    'superoffice_project',
    'superoffice_sale',

    'tribe_contact',
    'tribe_customer',
    'tribe_offer',
    'tribe_opportunity',
    'tribe_phase',
    'tribe_product',
    'tribe_product_line',

    'visma_salesorder',
    "visma_customerinvoice",
    "visma_inventory",

    'webkua_companies',
    'webkua_contractiteminfos',
    'webkua_contractitems',
    'webkua_contracts',
    'webkua_licenses',
    'webkua_productgroups',
    'webkua_products',
    'webkua_salesorders',
    "webkua_coursecatalogs",
    "webkua_courseregistrations",

    "afas_customer",
    "afas_mrr",
    "afas_contract"
]

# %%


def run(bronze_table, ignore_existing_tables=True):

    try:

        bronze_database = f"{env}_snowlake_bronze"

        table_name = f"jman_{bronze_table}"

        if check_table_exists(gc, env, db, table_name) and ignore_existing_tables:

            logger.info(f"{bronze_table} already exist ! Ignoring it because {ignore_existing_tables=}")
            return {
                    "table": bronze_table,
                    "status": "WARNING",
                    "message": f"{bronze_table} already exist ! Ignoring it because {ignore_existing_tables=}",
                    "traceback": traceback.format_exc(),
            }

        else:
            logger.info(f"Start {bronze_table}")

            try:
                logger.info(f"{bronze_table} Try reading table as non iceberg..")
                logger.info(f"{bronze_database}.{bronze_table}")
                df = spark.table(f"{bronze_database}.{bronze_table}")
            except Exception as e:
                logger.info(f"Could not read as non-iceberg - {e}")
                logger.info(f"{bronze_table} Try reading table as iceberg..")
                logger.info(f"{catalog}.{bronze_database}.{bronze_table}")
                df = spark.table(f"{catalog}.{bronze_database}.{bronze_table}")

            df = clean_column_names(df)

            dm = add_meta(df)

            write_s3(
                spark=spark,
                df=dm,
                env=env,
                db='external',
                table=table_name,
                method='overwrite',
                path_prefix='jman',
                partition_cols=[]
            )

            logger.info(f"{bronze_table} Done !")
            return {
                    "table": bronze_table,
                    "status": "SUCCESS",
            }

    except Exception as e:
        logger.info(f"{bronze_table} - error - {e}")
        return {
            "table": bronze_table,
            "status": "ERROR",
            "type": "Transformation error",
            "message": e,
            "traceback": traceback.format_exc(),
        }


# %%

if "tables" in args:
    tables = args["tables"].split(",")
else:
    tables = table_requirements

# %%

if is_running_locally():
    ignore_existing_tables = args.get("ignore_existing_tables", True)
    max_workers = args.get("max_workers", 8)
else:
    max_workers = args.get("max_workers", 15)
    ignore_existing_tables = args.get("ignore_existing_tables", False)

with ThreadPoolExecutor(max_workers=1) as executor:
    results = list(executor.map(lambda x: run(x, ignore_existing_tables), tables))


display_results(results, tables)

job.commit()

# %%
