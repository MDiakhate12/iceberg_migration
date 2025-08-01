# -*- coding: utf-8 -*-
# %%
import logging
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import etl_tools
import table_tools
from pyspark.sql import functions as F


args = etl_tools.get_args()
conf = etl_tools.configure_iceberg(
    env=args["environment"],
    db="bronze"
)

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
# current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
metadata.get("snapshots").show()
snapshot_datetime = F.to_date("committed_at", "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("committed_at")
latest_snapshot = (
    metadata
    .get("snapshots")
    .orderBy(snapshot_datetime.desc())
    .limit(1)
    .collect()
)
print(f"Latest snapshot: {latest_snapshot}")
last_snapshot_timestamp = latest_snapshot[0]["committed_at"]
latest_snapshot_id = latest_snapshot[0]["snapshot_id"]

zorder_sorting_column = args.get("zorder_sorting_column", "_ingest")  # Recommandé de mettre une colonne sur laquelle on fait beaucoup de filtres (pas forcément une colonne de date)
target_file_size_bytes = args.get("target_file_size_bytes", f'{200 * 1024 * 1024}')  # 200 Mo par défaut

# Afficher les informations du dernier snapshot
print(f"Latest snapshot ID: {latest_snapshot_id}")
print(f"Latest snapshot timestamp {last_snapshot_timestamp}")

# %%
# Appel de la méthode pour réécrire les fichiers de données avec Z-Ordering
metadata.call(
    "rewrite_data_files",
    options={
        "strategy": "'sort'",
        "sort_order": f"'{zorder_sorting_column} DESC NULLS LAST'",
        "options": f"""map(
                'min-input-files', '2',
                'target-file-size-bytes', '{target_file_size_bytes}'
            )"""
    }
).show()


# %%
# Expirer les snapshots plus anciens que la date actuelle
last_snapshot_timestamp_str = last_snapshot_timestamp.strftime('%Y-%m-%d %H:%M:%S')
metadata.call(
    "expire_snapshots",
    options={
        "older_than": f"TIMESTAMP '{last_snapshot_timestamp_str}'",
    }
).show()

# %%
# Supprimer les fichiers orphelins
metadata.call(
    "remove_orphan_files",
    options={
        "older_than": f"TIMESTAMP '{last_snapshot_timestamp_str}'",
    }
).show()

# %%
