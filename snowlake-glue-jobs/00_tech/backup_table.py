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


# JOB CONTEXT SETUP
args = etl_tools.get_args()
env = args["environment"]
db = args["db"]
desti = f"{args['table']}_backup"
conf = SparkConf()

conf = etl_tools.configure_iceberg(
    env,
    db,
    f'backup/{desti}'
)
if etl_tools.is_running_locally():
    conf.set("spark.driver.memory", "28g").set("spark.executor.memory", "28g")

sc = SparkContext.getOrCreate(conf=conf)
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session
job.init(args["JOB_NAME"], args)

catalog = f"{args.get('catalog')}." if args.get("catalog") not in 'None' else ''

logging.info(f"Copied table : {catalog}{env}_snowlake_{db}.{args['table']}")
logging.info(f"Destination table : {catalog}{env}_snowlake_{db}.{desti}")

df = spark.table(f"{catalog}{env}_snowlake_{db}.{args['table']}")


# %%
describe_result = spark.sql(f"DESC {catalog}{env}_snowlake_{db}.{args['table']}")
partition_columns = []
in_partition_section = False

# Loop through each row to find the partition column information

for row in describe_result.collect():
    if row.col_name in ["# Partition Information", "# Partitioning"]:
        in_partition_section = True
        # Skip the two headers
        continue
    if in_partition_section:
        if not (row.col_name).startswith('# '):
            value = row.data_type if args["catalog"] in "iceberg_catalog" else row.col_name
            partition_columns.append(value)
        elif row.col_name not in "# col_name":
            break
# %%
print(partition_columns)


table_tools.write_iceberg(
    df=df,
    table_name=desti,
    glue_context=gc,
    spark=spark,
    write_mode=table_tools.WriteMode.OVERWRITE,
    partitionedBy=partition_columns,
    orderedBy=partition_columns,
    tableProperties={
        'write.spark.accept-any-schema': 'true'
    },
    options={
        "mergeSchema": "true"
    },
)

logging.info("Backup Generated !")
# %%

# %%
