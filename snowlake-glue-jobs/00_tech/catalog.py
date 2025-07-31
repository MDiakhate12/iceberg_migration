# -*- coding: utf-8 -*-
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from etl_tools import write_s3
import pandas as pd
import boto3

# CONTEXT
args = getResolvedOptions(sys.argv, ["JOB_NAME", "environment"])
sc = SparkContext()
gc = GlueContext(sc)
job = Job(gc)
spark = gc.spark_session
session = boto3.Session()
glue = session.client('glue')
job.init(args["JOB_NAME"], args)
env = args["environment"]
db = 'gold'
desti = 'catalog'

# FUNCTIONS


def get_database_catalog(env, db, _next_token=''):
    catalog = []

    if _next_token is None:
        return catalog

    response = glue.get_tables(
        DatabaseName=f'{env}_snowlake_{db}',
        MaxResults=100,  # max = 100
        NextToken=_next_token
    )

    _next_token = response['NextToken'] if 'NextToken' in response else None

    for table in response['TableList']:
        for column in table['StorageDescriptor']['Columns']:
            catalog.append({
                'environment': env,
                'database_name': table['DatabaseName'],
                'table_name': table['Name'],
                'table_created': table['CreateTime'],
                'table_updated': table['UpdateTime'],
                'table_accessed': table['LastAccessTime'] if 'LastAccessTime' in table else None,
                'table_location': table['StorageDescriptor']['Location'],
                'table_input_format': table['StorageDescriptor']['InputFormat'] if 'InputFormat' in table else None,
                'table_output_format': table['StorageDescriptor']['OutputFormat'] if 'OutputFormat' in table else None,
                'table_count': table['StorageDescriptor']['Parameters']['recordCount'] if 'Parameters' in table['StorageDescriptor'] and 'recordCount' in table['StorageDescriptor']['Parameters'] else None,
                'table_size': table['StorageDescriptor']['Parameters']['sizeKey'] if 'Parameters' in table['StorageDescriptor'] and 'sizeKey' in table['StorageDescriptor']['Parameters'] else None,
                'table_partitions': ','.join([x['Name'] for x in table['PartitionKeys']]) if 'PartitionKeys' in table else None,
                'column_name': column['Name'],
                'column_type': column['Type']
            })

    return catalog + get_database_catalog(env, db, _next_token)


def compare_schema(row):
    if row['table_name'] is None:
        return f"Missing table {row['table_name_inte']} in {env}"
    elif row['table_name_inte'] is None:
        return "Table not existing in inte"
    elif row['column_name'] is None:
        return f"Missing column {row['table_name']}.{row['column_name_inte']} in {env}"
    elif row['column_name_inte'] is None:
        return "Column not existing in inte"
    elif row['column_type'] != row['column_type_inte']:
        return f"Column is typed {row['column_type_inte']} in inte"
    else:
        return "Same as inte"


# EXTRACT

# Récupération du schéma de l'env actuel et de l'inte
df_env = pd.DataFrame(
    get_database_catalog(env, 'bronze') + get_database_catalog(env, 'silver') + get_database_catalog(env, 'gold') + get_database_catalog(env, 'external')
)

df_inte = pd.DataFrame(
    get_database_catalog('inte', 'bronze') + get_database_catalog('inte', 'silver') + get_database_catalog('inte', 'gold') + get_database_catalog('inte', 'external')
)

# Jointure et comparaison du schéma de l'env avec celui de l'inte
df_inte = df_inte.add_suffix('_inte')
df = pd.merge(
    df_env,
    df_inte,
    left_on=['environment', 'database_name', 'table_name', 'column_name'],
    right_on=['environment_inte', 'database_name_inte', 'table_name_inte', 'column_name_inte'],
    how='outer'
)

df['inte_comparison'] = df.apply(compare_schema, axis=1)
df.drop(list(df.filter(regex='_inte')), axis=1, inplace=True)
df = df.sort_values(by=['environment', 'database_name', 'table_name', 'column_name'], ascending=True)

# Meta
df['_ingest'] = pd.Timestamp.now()

# LOAD
ds = spark.createDataFrame(df.astype(str).replace(['nan', 'NaT'], ''))
write_s3(spark=spark,
        df=ds,
        env=env,
        db=db,
        table=desti,
        method='overwrite',
        partition_cols=[])
job.commit()
