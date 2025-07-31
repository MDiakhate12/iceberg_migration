# -*- coding: utf-8 -*-
import sys
import boto3
import botocore
import logging
import json
from datetime import datetime
from pathlib import Path
import botocore.exceptions
from pyspark.conf import SparkConf
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from tenacity import retry, wait_exponential
from pyspark.sql.types import ArrayType, StructType, StringType
from pyspark.sql.functions import from_json, col, get_json_object, cast
from typing import Tuple, List
from botocore.exceptions import NoCredentialsError
from awsglue.utils import getResolvedOptions


class LoggerWithTraceback(logging.Logger):
    def error(self, msg, *args, **kwargs):
        # Ajouter exc_info=True par défaut
        kwargs['exc_info'] = True
        super().error(msg, *args, **kwargs)


# Remplacer le logger par défaut
logging.setLoggerClass(LoggerWithTraceback)

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# A METTRE DANS UNE FONCTION init_session A APPELER DANS LES JOBS
session = boto3.Session()
s3 = session.client('s3')
s3r = session.resource('s3')
athena = session.client('athena')
glue = session.client('glue')


# CHECK
def check_table_exists(gc, env, db, table):
    exists = False

    tables = gc.tableNames(f'{env}_snowlake_{db}')
    if table in tables:
        exists = True

    return exists


def check_table_type(env, db, table):
    table_version = glue.get_table(
        DatabaseName=f'{env}_snowlake_{db}',
        Name=table
    )

    try:
        table_type = table_version['Table']['Parameters']['table_type']
    except KeyError:
        table_type = None

    return table_type


# CLEAN
def drop_null_columns(df):
    """
    Drops columns containing only null values.
    :param df: PySpark DataFrame
    """
    _df_length = df.count()
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    to_drop = [k for k, v in null_counts.items() if v >= _df_length]
    df = df.drop(*to_drop)

    return df


# EXTRACT
def spark_sql(gc, spark, query: str, mapping: dict, transformation_ctx: str, dynamic: bool = False) -> DynamicFrame:
    """Retourne un DataFrame OU Dynamic Frame à partir d'une requête SparkSQL

    Args:
        gc (_type_): Contexte Glue
        spark (_type_): Contexte Spark
        query (_type_): Requete a executer
        mapping (dict): Mapping a applique sur la requete
        transformation_ctx (str): contexte glue
        dynamic (bool): True retourne un Dynamic Frame; False retoure un DF
    """
    for alias, frame in mapping.items():
        frame.createOrReplaceTempView(alias)
    result = spark.sql(query)

    if dynamic:
        return DynamicFrame.fromDF(result, gc, transformation_ctx)
    else:
        return result


@retry(wait=wait_exponential(multiplier=0.2, min=0.1, max=30))
# def poll_status(athena, _id):  # INITIER LA SESSION DANS LE JOB ET L'INJECTER DANS LA TOOL
def poll_status(_id):
    '''
    Implements exponential backoff on athena query execution
    '''
    q_execution = athena.get_query_execution(QueryExecutionId=_id)
    state = q_execution['QueryExecution']['Status']['State']
    if state == 'SUCCEEDED':
        return q_execution
    elif state == 'FAILED':
        return q_execution
    else:
        raise Exception


def get_attr_val(env, db, table, col, func, where=None):
    '''
    Returns func(column) value [min, max, ..] given context
    '''

    q = f'SELECT {func}({col}) FROM "{table}"'
    if where:
        q = f'{q} WHERE {where}'

    q_start = athena.start_query_execution(
        QueryString=q,
        QueryExecutionContext={
            'Database': f"{env}_snowlake_{db}",
        },
        ResultConfiguration={
            'OutputLocation': f's3://sglk-snowlake-{env}-eu-west-1/athena-results/'
        },
        WorkGroup=f'snowlake_{env}_workgroup'
    )

    q_status = poll_status(q_start['QueryExecutionId'])

    if q_status['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        # print(q_status['QueryExecution']['Statistics'])
        try:
            q_result = athena.get_query_results(QueryExecutionId=q_start['QueryExecutionId'])
            q_data = q_result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
            return q_data
        except Exception:
            return None
    else:
        return None


def table_has_column(env: str, db: str, table: str, col: str):
    """Retourne True la db.table contient la colonne donnée en paramètre

    Args:
        env (str): environment. Ex: 'inte'
        db (str): suffixe de la base. Ex: 'bronze'
        table (str): nom de la table. Ex: 'devis'
        col (str): nom de la colonne à chercher dan la table. Ex: 'iddevis'
    """

    table_cols = []
    try:
        table_cols = glue.get_table(
            DatabaseName=f'{env}_snowlake_{db}',
            Name=table,
        )["Table"]["StorageDescriptor"]["Columns"]
    except Exception:
        return None

    return (any(c["Name"] == col for c in table_cols))


def get_bronze_path(db: str, env: str):
    """Récupère le prefix S3 des bases exportées

    Args:
        db (str): nom de la base à identifier. Ex: 'scream'
        env (str): environnment. Ex: 'inte'
    """
    response = s3.list_objects_v2(
        Bucket=f'sglk-snowlake-{env}-eu-west-1',
        Delimiter='/',
        Prefix=f'bronze/{db}/'
    )

    try:
        prefix = response['CommonPrefixes'][0]['Prefix']
        path = f'{prefix}{db}/'
    except KeyError:
        path = None

    return path


def list_bronze_objects(env, db, schema, table, slicing):
    """Returns list of selected bronze objects

    Args:
        - env (str): environment
        - db (str): nom de la base. Ex: 'scream'
        - schema (str): nom du schema. Ex: 'scream'
        - table (str): nom de la table. Ex: 'conso_conso'
        - slicing (slice): slice constructor. For last, pass `slice(-1, None)`

    Returns:
        list: list of S3 objects
    """
    snowlake = f'sglk-snowlake-{env}-eu-west-1'
    bucket = s3r.Bucket(snowlake)
    prefix = get_bronze_path(db, env) + f"{schema}.{table}/"
    objects = bucket.objects.filter(Prefix=prefix)
    filtered = [f's3://{snowlake}/{o}' for o in [o.key for o in objects][slicing]]
    return filtered


def list_objects(env, prefix):
    """Returns list of objects in given Snowlake prefix

    Args:
        env str: environnement
        prefix str: prefix complet ex: 'external/test/'
    """
    snowlake = f'sglk-snowlake-{env}-eu-west-1'
    bucket = s3r.Bucket(snowlake)
    return bucket.objects.filter(Prefix=prefix)


def add_hash(df):
    """Adds MD5 hash to all df columns except "_" prefixes

    Args:
        df (_type_): DataFrame
    """
    return df.withColumn('_hash', F.md5(F.concat_ws('||', *[c for c in df.columns if not c.startswith('_')])))


def add_meta(df, hash=False, **additional_fields):
    """Adds metadata to dataframe

    Args:
        df (_type_): DataFrame
        hash: Add an _hash column
        additional_fields: Add each key column with given value (col name prefixed by _)
    """
    if hash:
        df = add_hash(df)

    df = (
        df.withColumn('_ingest', F.from_utc_timestamp(F.current_timestamp(), "Europe/Paris"))
        .withColumn('_src', F.input_file_name())
    )

    for field, value in additional_fields.items():
        if isinstance(value, type(F.col(""))):
            typed_value = value
        else:
            typed_value = F.lit(value)
        df = df.withColumn(f'{field}', typed_value)

    return df


def add_dt_part(df, dt_col, dt_format='yyyy-MM-dd HH:mm:ss', grain='M'):
    """Adds partition attributs to spark DataFrame

    Args:
        df (_type_): DataFrame
        dt_col (str): date column name
        dt_format (str, optional): Date formatting. Defaults to 'yyyy-MM-dd HH:mm:ss'.
        grain (str, optional): Year ('Y'), Month ('M') or Day ('D') granularity. Defaults to 'M'.
    """

    if grain == 'Y':
        new = (
            df.withColumn("date_col", F.to_date(F.col(dt_col), format=dt_format))
            .withColumn("_year", F.year(F.col("date_col")).cast("int"))
            .drop(F.col("date_col"))
        )
        return new

    elif grain == 'M':
        new = (
            df.withColumn("date_col", F.to_date(F.col(dt_col), format=dt_format))
            .withColumn("_year", F.year(F.col("date_col")).cast("int"))
            .withColumn("_month", F.month(F.col("date_col")).cast("int"))
            .drop(F.col("date_col"))
        )
        return new

    elif grain == 'D':
        new = (
            df.withColumn("date_col", F.to_date(F.col(dt_col), format=dt_format))
            .withColumn("_year", F.year(F.col("date_col")).cast("int"))
            .withColumn("_month", F.month(F.col("date_col")).cast("int"))
            .withColumn("_day", F.dayofmonth(F.col("date_col")).cast("int"))
            .drop(F.col("date_col"))
        )
        return new
    else:
        print('Choose Y|M|D grain')


def resolve_ca_partition(env, spark, val):
    """Defines _year and _month partition for given _ingest

    Args:
        env: environement
        spark: spark context
        val (datetime.datetime): _ingest
    """

    dt_cloture_ca = spark.sql(f"""
    SELECT
        dt_achat
        , dt_conso
    FROM {env}_snowlake_external.dt_cloture_ca
    """)

    cloture = (dt_cloture_ca
        .where(val <= dt_cloture_ca.dt_achat)
        .agg({"dt_conso": "min"})
        .first()[0]
    )

    return cloture.year, cloture.month


def resolve_iteration_partition(env, db, table, id_col):
    """Returns tuple with (max_iter, max_id) for partitionning by job iteration
    """

    max_iter = get_attr_val(env, db, table, '_iteration', 'max') or 0
    max_id = get_attr_val(env, db, table, id_col, 'max') or 0

    return (int(max_iter), int(max_id))


def extract_date_from_string(df, source_col, target_col):
    date_pattern = r"((0[1-9]|[12][0-9]|3[0-1])\/(0[1-9]|1[0-2])\/([0-9]{4}))"
    df = df.withColumn(target_col, F.regexp_extract(F.col(source_col), date_pattern, 1))
    df = df.withColumn(target_col, F.to_date(F.col(target_col), "dd/MM/yyyy"))
    return df


# TRANSFORM


def flatten_dataframe(nested_df, lowercase=True):
    """Flatten and return the given nested dataframe
       Columns trramming : data.element.Id => data_element_Id

    Args:
        nested_df : DataFrame
        lowercase : Lowercase columns name
    """

    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()
        for column_name, column_type in df.dtypes:
            if column_type[:6] == "struct":
                projected_df = df.select(column_name + ".*")
                stack.append((parents + (column_name,), projected_df))
            elif column_type[:5] == "array":
                print(f"Field {column_name} (under {parents}) ignored because its an array")
            else:
                new_column_name = column_name.lower() if lowercase else column_name
                columns.append(F.col(".".join(parents + (column_name,))).alias("_".join(parents + (new_column_name,))))

    return nested_df.select(columns)


# LOAD
def write_s3(spark, df, env, db, table, method='overwrite', compression='snappy', partition_cols=[], catalog=True, path_prefix=''):
    """Writes dynamic frame to s3. `overwrite` deletes `table` prior to load, `append` deletes common partitions.

    Args:
    - spark: spark context
    - df (DataFrame): Spark DataFrame
    - env (str): environment Ex: 'inte'
    - db (str): database Ex: 'silver'
    - table (str): s3 suffix Ex: 'table'
    - method (str): 'overwrite' | 'append'
    - compression (str): 'snappy' for HOT data | 'gzip' for COLD data
    - partition_cols (list): columns to partition by Ex: ['_year', '_month']
    - catalog (bool): If True, writes to Glue Catalog
    - path_prefix (str): s3 prefix Ex: 'netsuite/'
    """

    path = f"s3://sglk-snowlake-{env}-eu-west-1/{db}/{path_prefix}/{table}/"
    destination = f'{env}_snowlake_{db}.{table}'

    if method == 'overwrite':
        spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'static')
    elif method == 'append':
        spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    else:
        print("Valid `method` are: 'overwrite' | 'append'")
        return

    node = (df.write
              .option("compression", compression)
              .option("path", path)
              .mode('overwrite')
              .format("parquet")
            )

    if partition_cols:
        node.partitionBy(*partition_cols)

    if catalog:
        node.saveAsTable(destination)

    return


def delete_s3(env, path):
    """Delete objects in prefix

    Args:
        env (_type_): environment. Ex: 'inte'
        path (_type_): chemin à supprimer. Ex: 'silver/test'
    """
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=f"sglk-snowlake-{env}-eu-west-1", Prefix=path)

    for page in page_iterator:
        if "Contents" in page:
            param = [{"Key": x["Key"]} for x in page["Contents"]]
            try:
                if len(param) != 0:
                    s3.delete_objects(
                        Bucket=f"sglk-snowlake-{env}-eu-west-1",
                        Delete={
                            'Objects': param
                        }
                    )

            except botocore.exceptions.ClientError as error:
                raise error


def flatten_schema(schema, separator="_"):
    """
    Recursively flatten a dataframe containing Struct elements

    Params:
    schema (dict): Schema of the DataFrame in input

    Returns:
    prefixed_fields (list): List of all fields to select in the dataframe and their path in the nested Struct to get it
    """

    def recursive_flatten_schema(schema, prefix=None):
        fields = []

        for field in schema.fields:
            name = prefix + '.' + field.name if prefix else field.name

            dtype = field.dataType

            if isinstance(dtype, ArrayType):
                dtype = dtype.elementType

            if isinstance(dtype, StructType):
                fields += recursive_flatten_schema(dtype, prefix=name)
            else:
                fields.append(name)

        return fields

    flat = recursive_flatten_schema(schema)

    prefixed_fields = map((lambda name: F.col(name).alias(name.replace(".", separator))), flat)

    return list(prefixed_fields)


def get_array_columns_recursive(fields, parent=None):
    """
    Return all array type columns of the dataframe
    Args:
        schema (pyspark.sql.types.StructType): Schema of the dataframe
    Returns:
        list: List of array columns names
    """
    array_fields = []

    for field in fields:
        dtype = field.dataType

        if isinstance(dtype, ArrayType) and dtype.elementType not in (StructType, ArrayType):
            name = f"{parent.name}.{field.name}" if parent else field.name
            array_fields.append(name)

        if isinstance(dtype, StructType):
            logger.info(f"Encountered StructType {field.name} with fields {dtype.fieldNames()}. Looking for array columns inside...")
            subarray_fields = get_array_columns_recursive(dtype.fields, parent=field)
            array_fields.extend(subarray_fields)

    return array_fields


def get_array_columns(df):
    """
    Return all array type columns of the dataframe
    Args:
        schema (pyspark.sql.types.StructType): Schema of the dataframe
    Returns:
        list: List of array columns names
    """

    schema = df.schema

    return [field.name for field in schema.fields if isinstance(field.dataType, ArrayType)]


def explode_array_columns(df, array_columns, explode_function=F.explode_outer) -> DataFrame:
    """
    Explose toutes les colonnes de type array du DataFrame.

    Args:
        df (pyspark.sql.DataFrame): Le DataFrame Spark contenant des colonnes de type array.
        array_columns (list): Une liste des noms des colonnes de type array à exploser.

    Returns:
        pyspark.sql.DataFrame: Le DataFrame avec les colonnes de type array explodées.
    """
    for column in array_columns:
        df = df.withColumn(
            column,
            explode_function(column),
        )
    return df


def flatten_struct_columns(df, explode_arrays=True, flatten_cols=None, explode_columns=None, explode_function=F.explode_outer, separator="_") -> DataFrame:

    """
    Aplatit un DataFrame Spark contenant des données JSON imbriquées.

    Args:
        df (pyspark.sql.DataFrame): Le DataFrame Spark à aplatir.

    Returns:
        pyspark.sql.DataFrame: Le DataFrame aplati.
    """

    array_columns = get_array_columns(df)

    if flatten_cols:
        flatten_columns = [
            *flatten_schema(df.select(flatten_cols).schema, separator=separator),
            *[F.col(c) for c in df.columns if c not in flatten_cols]
        ]
    else:
        flatten_columns = flatten_schema(df.schema, separator=separator)

    if explode_arrays:

        if explode_columns:
            array_columns = [col for col in array_columns if col in explode_columns]

        exploded_df = explode_array_columns(df, array_columns, explode_function)

        flatten_df = exploded_df.select(flatten_columns)

        array_columns = get_array_columns(flatten_df)

        if array_columns:
            flatten_df = explode_array_columns(flatten_df, array_columns, explode_function)

        return flatten_df

    else:

        return df.select(flatten_columns)


def write_json(data, path, s3_client=None, bucket_name=None):
    """
    Écrit un dictionnaire Python au format JSON dans un fichier local ou un chemin S3.

    :param data: dict, les données à écrire
    :param path: str, chemin local ou S3 (ex: "s3://bucket_name/path/to/file.json")
    :param s3_client: boto3.client, optionnel, pour gérer S3 si nécessaire
    :param bucket_name: str, optionnel, nom du bucket S3 si le chemin ne le contient pas déjà
    """
    # Vérifier si le chemin est un chemin S3
    if path.startswith("s3://"):
        # Si le client S3 n'est pas fourni, en initialiser un
        if not s3_client:
            s3_client = boto3.client("s3")

        # Extraire le bucket et le chemin
        if not bucket_name:
            bucket_name, s3_path = path[5:].split("/", 1)
        else:
            s3_path = path

        try:
            # Convertir les données en JSON
            json_data = json.dumps(data, indent=4)

            # Écrire sur S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_path,
                Body=json_data,
                ContentType="application/json"
            )
            logger.info(f"Fichier écrit avec succès sur S3 : s3://{bucket_name}/{s3_path}")

        except NoCredentialsError:
            logger.info("Erreur : Aucune configuration d'identifiants AWS trouvée.")
        except Exception as e:
            logger.info(f"Erreur lors de l'écriture sur S3 : {e}")

    else:
        # Écrire un fichier local
        try:
            # Créer les répertoires si nécessaire
            local_path = Path(path)
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Écrire les données JSON dans un fichier local
            with open(local_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            logger.info(f"Fichier écrit avec succès localement : {local_path}")

        except Exception as e:
            logger.info(f"Erreur lors de l'écriture locale : {e}")


def coalesce_dataframe(df, target_parquet_size_mb=256) -> DataFrame:
    """
    Coalesce un DataFrame Spark en un nombre optimal de partitions, chaque partition ayant environ `target_parquet_size_mb`.

    :param df: pyspark.sql.DataFrame, le DataFrame Spark
    :param target_parquet_size_mb: int, taille cible en Mo par partition
    :return: pyspark.sql.DataFrame, DataFrame coalescé
    """
    # Étape 1: Calculer la taille totale estimée en octets
    # Utiliser un calcul approximatif de la taille des partitions
    total_size_in_bytes = df.rdd.mapPartitions(
        lambda iter_partition: [sum(len(str(row)) for row in iter_partition)]
    ).sum()

    # Étape 2: Calculer le nombre de partitions nécessaires
    target_partition_size_bytes = target_parquet_size_mb * 1024 * 1024
    num_partitions = max(1, int(total_size_in_bytes / target_partition_size_bytes))

    logger.info(f"Taille totale estimée : {total_size_in_bytes / (1024 * 1024):.2f} MB")
    logger.info(f"Nombre de partitions cibles : {num_partitions}")

    # Étape 3: Coalescer le DataFrame
    if num_partitions < df.rdd.getNumPartitions():
        return df.coalesce(num_partitions)
    else:
        return df.repartition(num_partitions)


def configure_iceberg(env, db, folder=None):
    """Configure Iceberg catalog and warehouse

    Args:
        env (str): environment
        db (str): database
        folder (str): S3 output folder

    Returns:
        tuple: (SparkConf, str, str, str)
    """
    aws_region = "eu-west-1"
    s3_bucket_name = f"sglk-snowlake-{env}-{aws_region}"
    s3_bucket_path = f"s3://{s3_bucket_name}"
    db_s3_path = f"{s3_bucket_path}/{db}"

    if not folder:
        data_s3_path = db_s3_path
    else:
        data_s3_path = f"{db_s3_path}/{folder}"
    iceberg_catalog_name = "iceberg_catalog"
    iceberg_database_name = f"{env}_snowlake_{db}"

    conf = SparkConf()
    conf.set("spark.sql.packages", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set(f"spark.sql.catalog.{iceberg_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{iceberg_catalog_name}.warehouse", data_s3_path)
    conf.set(f"spark.sql.catalog.{iceberg_catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set(f"spark.sql.catalog.{iceberg_catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set(f"spark.sql.catalog.{iceberg_catalog_name}.glue.region", aws_region)
    conf.set("spark.hadoop.aws.region", aws_region)

    conf.set("spark.environment.env", env)
    conf.set("spark.environment.db", db)
    conf.set("spark.environment.folder", folder)
    conf.set("spark.environment.catalog_name", iceberg_catalog_name)
    conf.set("spark.environment.database_name", iceberg_database_name)
    conf.set("spark.environment.s3_bucket_path", s3_bucket_path)
    conf.set("spark.environment.db_s3_path", db_s3_path)
    conf.set("spark.environment.data_s3_path", data_s3_path)
    conf.set("spark.environment.bucket_name", s3_bucket_name)

    if is_running_locally():
        logger.info("Running locally.. Setting memory to 28g.")
        conf.set("spark.driver.memory", "28g").set("spark.executor.memory", "28g")

    logger.info(f"Configured iceberg to write at {data_s3_path} in catalog {iceberg_catalog_name}")
    logger.info(f"Tables will have the following format {iceberg_catalog_name}.{iceberg_database_name}.<table_name>")

    return conf


def is_running_locally():
    """Check if the script is running locally or in Glue

    Returns:
        bool: True if running locally, False otherwise
    """

    try:
        getResolvedOptions(sys.argv, ["JOB_NAME"])
        return False
    except Exception:
        return True


def get_args(**extra_args):

    """Get Glue job parameters (handle local case and remote case)
    In local, creates a mock job named 'local_job_test' in 'inte' environment.

    Returns:
        dict: Glue job parameters
    """
    is_locally_running = is_running_locally()
    extras = [f"{'--' if is_locally_running else ''}{k}={v}" for k, v in extra_args.items()]

    if is_locally_running:
        if "env" in extra_args:
            env = extra_args.pop("env")

            if env == "prod" and is_locally_running:
                result = input("""
################################ WARNING ################################
You've set environment to prod !
Code will execute with production data !
Data Loss might happen if not handled properly !
Are you sure ? Type 'yes' to continue...
                      """)
                if result != "yes":
                    logger.info("Exiting run...")
                    sys.exit(1)
        else:
            env = "inte"

        args = sys.argv + ["--JOB_NAME", "local_job_test", "--environment", env, *extras]

        logging.info(f"{args=}")
        logging.info(f"{extra_args=}")

        return getResolvedOptions(args, ["JOB_NAME", "environment", *extra_args.keys()])

    else:
        RESERVED_ARGS = (
            "--job_id",
            "--job_run_id",
            "--job-bookmark-option",
            "--f",
            "--additional-python"
        )

        args = sys.argv

        captured_args = [
            arg.replace("--", "") for arg in args[1:]
            if (
                arg.startswith(("--")) and
                not arg.lower().startswith(RESERVED_ARGS)
            )
        ]

        logging.info(f"{args=}")
        logging.info(f"{captured_args=}")
        logging.info(f"{extra_args=}")

        return getResolvedOptions(args, [*captured_args, *extra_args.keys()])


def pivot_table(df, keys):
    # Liste des colonnes à pivoter (excluant les clés primaires)
    value_columns = [c for c in df.columns if c not in keys]

    # Vérification pour éviter un stack vide
    if not value_columns:
        raise ValueError("Aucune colonne à pivoter : toutes les colonnes sont des clés.")

    # Construction de l'expression stack() avec correction des types
    unpivotExpr = f"""stack({len(value_columns)}, {", ".join(f"'{c}', `{c}`" for c in value_columns)}) AS (columns, nb_doublons)"""

    print(f"Expression générée pour stack(): {unpivotExpr}")

    return (
        df
        .groupBy(keys)
        .agg(
            *[
                F.size(F.collect_set(c)).alias(c)
                for c in value_columns
            ]
        )
        .select(*[f"`{k}`" for k in keys], F.expr(unpivotExpr))
    )


def list_columns_with_duplicates(df, primary_keys, condition=None) -> DataFrame:

    if condition:
        df = df.where(condition)

    df = pivot_table(df, primary_keys)

    return df.where("nb_doublons > 1").orderBy(primary_keys)


def select_columns_with_duplicates(df, primary_keys, condition="1=1", additionnal_columns=None) -> DataFrame:
    duplicate_cols = list_columns_with_duplicates(df, primary_keys, condition).select("columns").collect()

    duplicate_cols = [c["columns"] for c in duplicate_cols]

    duplicate_cols = list(set(duplicate_cols))

    logger.info(f"Primary keys : {primary_keys}")
    logger.info(f"Duplicate columns : {duplicate_cols}")

    if additionnal_columns:
        duplicate_cols.extend(additionnal_columns)

    return df.select(*primary_keys, *duplicate_cols).where(condition).orderBy(primary_keys)


def display_results(results, datasources, ignore_warning=True):

    errors = []
    error_tables = []
    error_count = 1

    warnings = []
    warning_tables = []
    warning_count = 1

    logger.info(f"Results : {results}")

    for output in results:
        logger.info(f"Inspecting result: {output['table']}")

        if output['status'] != "SUCCESS":
            logger.error(f"Ingestion of {output['table']} have failed with the following status {output['status']} and traceback {output['traceback']} !")
            count_type = warning_count if output['status'] == "WARNING" else error_count

            message = f"""

{count_type}) {output['status']} - ################################################################################################
TABLE     - {output['table']} - {output.get('table', 'N/A')}
STATUS    - {output['table']} - {output.get('status', 'N/A')}
TYPE      - {output['table']} - {output.get('type', 'N/A')}
MESSAGE   - {output['table']} - {output.get('message', 'N/A')}
TRACEBACK - {output['table']} - {output.get('traceback', 'N/A')}
FAIL PATH - {output['table']} - {output.get('failed_ingestions_staging_path', 'N/A')}"""

            if output['status'] == "ERROR":
                errors.append(message)
                error_tables.append(output['table'])
                error_count += 1

            if output['status'] == "WARNING":
                warnings.append(message)
                warning_tables.append(output['table'])
                warning_count += 1

    if warnings:
        warnings = "\n\n".join(warnings)

        if not ignore_warning:
            raise Exception(f"{len(warning_tables)}/{len(datasources)} datasource(s) ({warning_tables}) have failed with warnings ! {warnings}")

        logger.warning(f"{len(warning_tables)}/{len(datasources)} datasource(s) ({warning_tables}) have failed with warnings ! {warnings}")

    if errors:
        errors = "\n\n".join(errors)
        raise Exception(f"{len(error_tables)}/{len(datasources)} datasource(s) ({error_tables}) have failed with erros ! {errors}")
    else:

        try:
            result_message = json.dumps(results, indent=2)
        except Exception:
            result_message = results

        logger.info(f"All jobs have succeeded ! {result_message}")


def json_to_struct(df: DataFrame, column: str, spark_session: SparkSession, nested_object: str = None, col_name: str = None) -> DataFrame:
    """
        Convertis un json en struct dynamique
        df (spark DataFrame) : le Dataframe contenant la colonne
        col (str) : la colonne contenant le json
        nested_object (str optionnel) : Si le json est nesté et que
        l'on veut un niveau en particulier, le spécifier dans ce paramètre
        col_name (str optionnel) : Si précisé, une nouvelle colonne sera créée,
        sinon la colonne faisant référence à l'objet sera remplacée
    """
    final_col = col_name if col_name else column

    if nested_object:
        df = df.withColumn(final_col, get_json_object(col(column), f"{nested_object}"))

    json_rdd = df.select(final_col).rdd.map(lambda row: row[final_col])
    df_json = spark_session.read.json(json_rdd)
    schema = df_json.schema

    df = df.withColumn(final_col, from_json(col(final_col), schema))

    if col_name:
        df.drop(column)

    return df


# def jsons_to_structs(df, json_cols, spark, nested_fields=None, rename_map=None):
#     return reduce(lambda col, d : json_to_struct(d, col, spark,
#                                                  rename_map.get(col) if rename_map else None,
#                                                  nested_fields.get(col) if nested_fields else None),
#                   json_cols, df)


def add_date(
    datetime_string: str,
    years: int = 0,
    months: int = 0,
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0,
    microseconds: int = 0,
    output_format: str = '%Y-%m-%dT%H:%M:%S'
) -> str:

    """Add specified date to given datetime (input datetime must be in iso formatted date)

    Args:
        datetime_string (str): input datetime
        years (int): years to add
        months (int): months to add
        days (int): days to add
        hours (int): hours to add
        minutes (int): minutes to add
        seconds (int): seconds to add
        microseconds (int): microseconds to add
        output_format (str): output datetime format (default: '%Y-%m-%dT%H:%M:%S' eg: 2025-01-01T00:00:00)

    Returns:
        str: output datetime in given format
    """

    dt = datetime.fromisoformat(datetime_string)

    return datetime(
        dt.year + years,
        dt.month + months,
        dt.day + days,
        dt.hour + hours,
        dt.minute + minutes,
        dt.second + seconds,
        dt.microsecond + microseconds
    ).strftime(output_format)


def read_s3(bucket: str, key: str) -> bytes:
    """
    Lit un fichier depuis un bucket S3 et retourne son contenu en bytes.

    :param bucket: Nom du bucket S3
    :param key: Clé (chemin) de l'objet S3
    :return: Contenu du fichier en bytes
    :raises Exception: En cas d'erreur d'accès au fichier S3
    """
    s3 = boto3.client("s3")

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()  # ⚠ Charge tout en mémoire !
    except botocore.exceptions.ClientError as e:
        print(f"Erreur lors de la lecture du fichier {key} dans {bucket}: {e}")
        return b""  # Retourne un objet vide en cas d'erreur


def suggest_partition(df: DataFrame, cardinality_bounds: Tuple[int, int] = None, min_occurences: int = 100, skew_treshold: float | int = None) -> List[dict[str:str | int]]:
    """
        Returns list of appropriate columns
        (string and struct columns are filtered out by default)
        to use as partitions
        sorted by several criteras :
            - approx_cardinalities (int, DESC) : The approximative count of distinct value
            - average_occurences (int, ASC): The average occurence
            - skew (float, DESC) : Goes from 0 to 1, the lowest it gets the better it is,
            since it means that the number of occurences are even for each cardinality

        Params:
            - df : The dataframe to get suggestions from
            - cardinality_bounds : Min and max cardinality bounds to accept
            - min_occurences: Minimum average occurence to accept
            - skew_treshold : Maximum treshold to accept
    """

    def can_cast_to_int(col_name):
        try:
            df_casted = df.select(col(col_name).cast("int"))
            return df_casted.filter(col(col_name).isNotNull()).count() > 0
        except Exception as e:
            print(e)
            return False

    suggestions = []
    row_count = df.count()
    appropriate_columns = [col for col in df.columns if 'boolean' not in dict(df.dtypes).get(col, '') and can_cast_to_int(col)]
    print(appropriate_columns)
    for column in appropriate_columns:
        approx_distinct = df.agg(approx_count_distinct(column)).collect()[0][0]
        average_count = math.ceil(row_count / approx_distinct if approx_distinct > 0 else 1)
        skew_ratio = (row_count / approx_distinct) / average_count if average_count > 0 else 1

        if cardinality_bounds\
            and (
                approx_distinct < cardinality_bounds[0]
                or
                approx_distinct > cardinality_bounds[1]):
            continue

        if min_occurences and average_count < min_occurences:
            continue

        if skew_treshold and skew_ratio > skew_treshold:
            continue

        suggestion = {
            "column": column,
            "approx_cardinalities": approx_distinct,
            "average_occurences": average_count,
            "skew_ratio": skew_ratio
        }

        suggestions.append(suggestion)

    sorted_suggestions = sorted(suggestions, key=lambda x: (
            -x["approx_cardinalities"], x["average_occurences"], -x["skew_ratio"]
        )
    )

    return sorted_suggestions


def aggregate_columns(df: DataFrame, cols: List[Tuple[str, str]] | List[str], col_name: str) -> DataFrame:
    if isinstance(cols, list) and all(isinstance(item, tuple) and len(item) == 2 and all(isinstance(i, str) for i in item) for item in cols):
        key_value_columns = [col(original).cast(StringType()).alias(key_name) for original, key_name in cols]
    else:
        key_value_columns = [col(c).cast(StringType()).alias(c) for c in cols]

    return df.withColumn(f"{col_name}", F.struct(*key_value_columns))
