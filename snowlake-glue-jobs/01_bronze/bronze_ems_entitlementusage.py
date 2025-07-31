# -*- coding: utf-8 -*-
# %%
import logging
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
import etl_tools
import table_tools
import datasources

args = etl_tools.get_args()
conf = etl_tools.configure_iceberg(env=args["environment"], db="bronze", folder="ems_geodesial")

# conf.set("spark.executor.memory", "29g").set("spark.driver.memory", "29g")
# conf.set("spark.memory.fraction", "0.8")

sc = SparkContext(conf=conf)
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session

job.init(args["JOB_NAME"], args)

logger = logging.getLogger(__name__)

data_s3_path = spark.conf.get("spark.environment.data_s3_path")
db_s3_path = spark.conf.get("spark.environment.db_s3_path")

df = spark.read.option("header", "true").load(
    f"{db_s3_path}/tmp_row_data/ems_extract_geodesial/final"
)
df.rdd.getNumPartitions()
# %%

columns_to_snake_case = [
    F.col(c).alias(c.strip().lower().replace(" ", "_")) for c in df.columns
]

for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))

df.printSchema()

# %%

df = etl_tools.add_meta(df)

datasource = datasources.EMSDataSources.EMS_ENTITLEMENTUSAGE

table_tools.write_iceberg(
    df=df,
    table_name=datasource.source_table_name,
    glue_context=gc,
    spark=spark,
    write_mode=datasource.write_mode["bronze"],
    orderedBy=["customer_id", "entitlement_id", "product_key"]
)
