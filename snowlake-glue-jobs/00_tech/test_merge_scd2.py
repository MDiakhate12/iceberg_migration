# -*- coding: utf-8 -*-
# %%
import logging
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import etl_tools
import table_tools
import pyspark.sql.functions as F
import datasources
from datetime import datetime


args = etl_tools.get_args()
conf = etl_tools.configure_iceberg(
    env=args["environment"],
    db="bronze",
    folder="tribe",
)

conf.set("spark.driver.memory", "28g")
conf.set("spark.memory.fraction", "0.9")

sc = SparkContext(conf=conf)
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session

job.init(args["JOB_NAME"], args)

env = args["environment"]

logger = logging.getLogger(__name__)

catalog = spark.conf.get("spark.environment.catalog_name")
database = spark.conf.get("spark.environment.database_name")

PARQUET_SIZE_MB_MAX = 256

write_mode = table_tools.WriteMode.SCD2
table_name = f'test_{write_mode}_evolution'
target_table_name = f"{catalog}.{database}.{table_name}"

# %%
spark.sql(f"DROP TABLE IF EXISTS {target_table_name}").show()
# %%

df = spark.createDataFrame(
    [
        ('toto', 12),
        ('alice', 34),
        ('Bob', 45)
    ],
    ('name', 'age')
)

df.show()

# %%

table_tools.write_iceberg(
    df=df,
    primary_keys='name',
    table_name=table_name,
    glue_context=gc,
    spark=spark,
    write_mode=write_mode,
    tableProperties={
        'write.spark.accept-any-schema': 'true'
    },
    options={
        "mergeSchema": "true"
    },
)

# %%
target = spark.table(target_table_name).where("_is_active = true")

target.show()
# %%

source = spark.createDataFrame(
    [
        ('toto', 555, 'M'),
        ('messi', 100, 'F')
    ],
    ('name', 'age', 'gender')
)

source = table_tools.add_tech_columns(source, ['name'], table_tools.WriteMode.SCD2)
source = source.select(source.columns)

tmp = (
    source.alias("updates")
    .join(target.alias("target"), "name")
    .where("updates._hash != target._hash")
)

updates_of_existing_lines = tmp.select("updates.*")
old_lines_to_close = tmp.select("target.*").withColumn("_is_active", F.lit(False)).withColumn("_end_date", F.current_timestamp())
new_lines = source.subtract(updates_of_existing_lines)

updates_of_existing_lines = updates_of_existing_lines.withColumn("_operation", F.lit("update")).withColumn("_merge_key", F.lit(None))
old_lines_to_close = old_lines_to_close.withColumn("_operation", F.lit("close")).withColumn("_merge_key", F.col("name"))
new_lines = new_lines.withColumn("_operation", F.lit("insert")).withColumn("_merge_key", F.lit(None))

# source.show()
# updates_of_existing_lines.show()
# new_lines.show()
# old_lines_to_close.show()

merge_source = updates_of_existing_lines.unionByName(old_lines_to_close, allowMissingColumns=True).unionByName(new_lines, allowMissingColumns=True)
merge_source.show()
merge_source.createOrReplaceTempView("source")
# %%
spark.sql(f"""
        MERGE INTO {target_table_name} AS target
        USING source AS source
        ON source._merge_key = target.name
        WHEN MATCHED AND target._hash != source._hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
""")
# %%
spark.table(target_table_name).printSchema()
spark.table(target_table_name).show()

# %%

primary_keys = datasources.TribeDataSources.CUSTOMER.primary_keys
primary_keys = ",".join(primary_keys)

# %%
metadata = table_tools.TableMetadata(
    spark=spark,
    catalog=catalog,
    database="inte_snowlake_bronze",
    table="ems_entitlementusage"
)

# Obtenir la date actuelle au format 'YYYY-MM-DD HH:MM:SS'
current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

print(current_time)

# %%
metadata.call(
    "rewrite_data_files",
    options={
        "strategy": "'sort'",
        "sort_order": "'start_date_time DESC NULLS LAST'",
        "options": f"""map(
                'min-input-files', '2',
                'target-file-size-bytes', {200 * 1024 * 1024}
            )"""
    }
).show()

# %%
# metadata.call(
#     "rewrite_data_files",
#     options={
#         "options": f"""map(
#             'min-input-files', '2',
#             'remove-dangling-deletes', 'true',
#             'target-file-size-bytes', {200 * 1024 * 1024}
#         )"""
#     }
# ).show()

# %%
