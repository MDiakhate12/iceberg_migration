# -*- coding: utf-8 -*-
# %%
import boto3
import logging
import pandas as pd
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import etl_tools
import time
import table_tools

# 0. Contexte

args = etl_tools.get_args()
conf = etl_tools.configure_iceberg(
    env=args["environment"],
    db="external",
    folder="jman_redshift_extract",
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

# Configuration
region = "eu-west-1"
cluster_id = "prod-redshift-jman"
redshift_database = "prod_jman"
db_user = "mouhammad.diakhate"  # mappé à Redshift

# Client Redshift Data
client = boto3.client("redshift-data", region_name=region)

# %%


def extract_redshift_table(
    table: str,
    cluster_id: str,
    database: str,
    db_user: str,
    table_tools=None,
    schema: str = "public",
    write_mode=table_tools.WriteMode.OVERWRITE,
):
    """
    Lit une table Redshift via Redshift Data API et retourne un DataFrame Spark.
    Optionnellement, écrit dans Iceberg si `write_iceberg=True`.
    """

    sql = f"SELECT * FROM {schema}.{table};"

    # 1. Lancer la requête
    response = client.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=sql
    )

    statement_id = response["Id"]
    print(f"Statement ID: {statement_id}")

    # 2. Attendre la fin de l'exécution
    while True:
        status = client.describe_statement(Id=statement_id)
        if status["Status"] in ["FINISHED", "FAILED", "ABORTED"]:
            break
        time.sleep(1)

    if status["Status"] != "FINISHED":
        raise Exception(f"Requête échouée : {status['Status']}")

    # 3. Récupérer les résultats
    result = client.get_statement_result(Id=statement_id)
    records = result["Records"]
    columns = [col["name"] for col in result["ColumnMetadata"]]

    # 4. Convertir en Pandas DataFrame
    rows = [
        [list(col.values())[0] if col else None for col in row]
        for row in records
    ]
    pandas_df = pd.DataFrame(rows, columns=columns)

    pandas_df = pandas_df.astype(str)

    # 5. Convertir en Spark DataFrame
    df = spark.createDataFrame(pandas_df)
    df.show()

    # 6. Écrire dans Iceberg si demandé
    table_tools.write_iceberg(
        df=df,
        table_name=table,
        glue_context=gc,
        spark=spark,
        write_mode=getattr(table_tools.WriteMode, write_mode.upper())
    )

    return df


# %%

tables = [
    "booking_no",
    "booking_fr",
    "booking_nl",
    "billing_no",
    "billing_fr",
    "billing_nl",
    "booking_consolidated",
    "billing_consolidated",
    "consumption_consolidated_fr_nl"
]

for table in tables:
    try:
        # Lire la table Redshift et écrire dans Iceberg
        df = extract_redshift_table(
            table=table,
            cluster_id=cluster_id,
            database=redshift_database,
            db_user=db_user,
            table_tools=table_tools,
        )
        logger.info(f"Table {table} lue avec succès et écrite dans Iceberg.")
    except Exception as e:
        logger.error(f"Erreur lors de la lecture de la table {table}: {e}")
        continue
# %%
