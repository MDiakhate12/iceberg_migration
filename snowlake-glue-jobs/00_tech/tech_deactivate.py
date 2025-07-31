# -*- coding: utf-8 -*-
# %%
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import unix_timestamp
import etl_tools
import logging
import table_tools
import datasources
import snowlake_api_tools
import constants
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F
from concurrent.futures import ThreadPoolExecutor
from functools import reduce
from pyspark.sql.types import StructType, StructField, StringType


# %%
# JOB CONTEXT SETUP
args = etl_tools.get_args(source="visma", table="GeneralLedgerTransactions")
env = args["environment"]
db = "bronze"
conf = SparkConf()

conf = etl_tools.configure_iceberg(
    env,
    db,
    folder="tech"
)
if etl_tools.is_running_locally():
    conf.set("spark.driver.memory", "28g").set("spark.executor.memory", "28g")

sc = SparkContext.getOrCreate(conf=conf)
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session
job.init(args["JOB_NAME"], args)

catalog = "iceberg_catalog"

if args["source"] == "netsuite":
    dl = datasources.NetsuiteDataSources
if args["source"] == "afas":
    dl = datasources.AfasDataSources
if args["source"] == "visma":
    dl = datasources.VismaDataSources

datasource = dl.get_one(args["table"])
datasource.min_disabled_date = 'tran.lastmodifieddate'
now = datetime.now()
min_disabled_date = (now - relativedelta(months=2)).replace(day=1).date()


# %%
def deactivate(datasource: datasources.Datasource, min_date=now):
    # connector, pagination, query, date = prepare_query(datasource)
    # source_df = collect_pages(connector, pagination, query).select(*datasource.primary_keys)
    source_filters, target_filters = datasource.prepare_filters(min_date)
    rows = datasource.collect_pks(source_filters)

    target_table_str = f"{catalog}.{env}_snowlake_bronze.{datasource.target_table_name}"
    target_table = spark.table(target_table_str).filter(target_filters)
    target_columns = target_table.columns

    schema = StructType([StructField(col, StringType(), True) for col in datasource.primary_keys])
    source_df = spark.createDataFrame(rows, schema)

    print(source_df.count())
    print(target_table.count())

    missing_df = (
            target_table.alias("t")
            .join(source_df.alias("s"), on=datasource.primary_keys, how="left_anti")
            .select([F.col(f"t.{col}") for col in target_columns if "_is_missing" not in col])
        )

    logging.info(f"{missing_df.count()} row(s) missing between target and source")

    table_tools.write_iceberg(
        df=missing_df,
        table_name="tmp_tech_check_missing",
        glue_context=gc,
        spark=spark,
        write_mode=table_tools.WriteMode.OVERWRITE,
        tableProperties={
            'write.spark.accept-any-schema': 'true'
        },
        options={
            "mergeSchema": "true"
        }
    )

    logging.info("Checking if '_is_missing' column exists")

    if "_is_missing" not in target_columns:
        logging.info("'_is_missing' non existent, adding it to the target table...")
        spark.sql(f"""
            ALTER TABLE {target_table_str}
                ADD COLUMN _is_missing BOOLEAN
        """)
        spark.sql(f"""
            UPDATE {target_table_str}
            SET _is_missing = FALSE
        """)

    logging.info("Deactivating missing PKs in source but not in target")
    pk_conditions = " AND ".join([f"t.{pk} = m.{pk}" for pk in datasource.primary_keys])
    spark.sql(f"""
        DELETE FROM {target_table_str} AS t
        WHERE EXISTS (
            SELECT 1
            FROM {catalog}.{env}_snowlake_bronze.tmp_tech_check_missing AS m
            WHERE {pk_conditions}
        )
    """)

    missing_df = missing_df.withColumn("_is_missing", F.lit(True))

    table_tools.write_iceberg(
        df=missing_df,
        table_name=datasource.target_table_name,
        glue_context=gc,
        spark=spark,
        write_mode=table_tools.WriteMode.APPEND,
        tableProperties={
            'write.spark.accept-any-schema': 'true'
        },
        options={
            "mergeSchema": "true"
        }
    )
    logging.info("Successfully deactivated missing PK in target")


# %%
deactivate(datasource)
# %%
