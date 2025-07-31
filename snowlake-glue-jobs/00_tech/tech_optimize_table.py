# -*- coding: utf-8 -*-
# %%
import logging
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import etl_tools
import table_tools
from datetime import datetime
from pyiceberg.catalog import load_catalog
from pyiceberg.types import IntegerType


args = etl_tools.get_args()
conf = etl_tools.configure_iceberg(
    env=args["environment"],
    db="bronze"
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
                'target-file-size-bytes', '{200 * 1024 * 1024}'
            )"""
    }
).show()

# %%
catalog = load_catalog("iceberg_catalog")
table = catalog.load_table("inte_snowlake_bronze.test_scd2_evolution")

# %%
print(table)

# %%
with table.update_schema() as update:
    update.add_column("weight", IntegerType(), "Poids")
# %%
print(table)
# %%
with table.update_schema() as update:
    update.move_after("weight", "age")

# %%
table.history()
# %%
table.snapshots()
# %%
df = spark.createDataFrame(
    [
        ('patrick', 12, 49, 'BLABLA'),
    ],
    ('name', 'age', 'weight', 'description_serieuse')
)

table_tools.write_iceberg(
    df=df,
    primary_keys=['name'],
    table_name="test_scd2_evolution",
    glue_context=gc,
    spark=spark,
    write_mode="scd2",
    tableProperties={
        'write.spark.accept-any-schema': 'true'
    },
    options={
        "mergeSchema": "true"
    },
)
# %%
table.current_snapshot()
# %%
table.snapshots()

# %%
metadata = table_tools.TableMetadata(spark, "iceberg_catalog", "inte_snowlake_bronze", "test_scd2_evolution")
# %%

spark.sql("""CALL iceberg_catalog.system.rollback_to_snapshot('inte_snowlake_bronze.test_scd2_evolution', 215792183996940713)""")

# %%

metadata.call("rollback_to_snapshot", options={
    "snapshot_id": "215792183996940713"
}).show()

# %%
catalog = load_catalog("iceberg_catalog")
table = catalog.load_table("inte_snowlake_bronze.test_scd2_evolution")

table.history()
# %%
print(table)
# %%
