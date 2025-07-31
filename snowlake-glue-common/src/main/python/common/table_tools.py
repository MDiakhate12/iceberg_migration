# -*- coding: utf-8 -*-
import logging
from typing import Literal
from dataclasses import dataclass, fields
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
import etl_tools
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


@dataclass
class WriteMode:
    SCD2 = "scd2"
    APPEND = "append"
    OVERWRITE = "overwrite"
    OVERWRITE_PARTITIONS = "overwritePartitions"
    UPSERT = "upsert"

    @classmethod
    def get_all() -> list:
        return [f.default for f in fields(WriteMode)]


def check_schema_change(target_df, final_df):

    target_schema_diff = {c for c in set(target_df.schema) - set(final_df.schema) if not c.name.startswith("_")}
    if target_schema_diff:
        logger.warning(f"{len(target_schema_diff)} columns have been changed or removed {target_schema_diff}")

    source_schema_diff = {c for c in set(final_df.schema) - set(target_df.schema) if not c.name.startswith("_")}
    if source_schema_diff:
        logger.warning(f"{len(target_schema_diff)} columns have been changed or added {source_schema_diff}")


def check_duplicates(df, primary_keys, table_name):
    df_grouped = df.groupBy(primary_keys).count()

    has_duplicates = (
        df_grouped
        .filter("count > 1")
        .limit(1)
        .count() > 0
    )

    if has_duplicates:

        logger.error(f"Found duplicated lines when using specified primary keys {primary_keys} for table {table_name} !")
        try:
            gdf = df_grouped.orderBy(F.desc("count"))
        except Exception:
            gdf.show()
        pass
        first_lines = gdf.select(primary_keys).limit(5).collect()[0].asDict()
        print(f"{first_lines=}")

        logger.error(f"Showing one example of duplicates with primary keys {primary_keys} for table {table_name}...")

        try:
            etl_tools.select_columns_with_duplicates(df, primary_keys).show()

        except Exception as e:
            logger.error(f"Could not show an example of line duplicates - {e}")

        return ValueError(
            f"Invalid primary keys {primary_keys} for table {table_name}!"
            " Found duplicated lines when using specified primary keys"
            " You should provide primary keys that ensure unicity of each line before writing your iceberg table"
        )
    else:
        logger.info(f"{table_name} has no duplicates.")
        return None


def get_attribute_columns(df, primary_keys):
    return [
        c
        for c in df.columns
        if c not in primary_keys and not c.startswith("_")
    ]


def generate_hash(attribute_columns, excluded_columns=None):
    coalesced_attribute_columns = [
        F.coalesce(F.col(col).cast("string"), F.lit(""))
        for col in sorted(attribute_columns)
        if col not in excluded_columns
    ]

    return F.md5(F.concat(*coalesced_attribute_columns))


def generate_concat(attribute_columns, excluded_columns=None):

    sorted_attribute_columns = sorted(attribute_columns)

    coalesced_attribute_columns = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in sorted_attribute_columns]

    if excluded_columns:
        coalesced_attribute_columns = [c for c in coalesced_attribute_columns if c not in excluded_columns]

    return F.concat(*coalesced_attribute_columns)


def add_tech_columns(df, primary_keys, write_mode=None, excluded_attribute_from_hash=None):

    # Add SCD2 Technical Columns
    attribute_columns = get_attribute_columns(df, primary_keys)

    df = df.withColumn("_is_deleted", F.lit(False))  # Add _is_deleted column

    if write_mode == WriteMode.SCD2:

        return (
            df.withColumn("_start_date", F.current_timestamp())
            .withColumn("_end_date", F.lit(None).cast(TimestampType()))
            .withColumn("_is_active", F.lit(True))
            .withColumn("_hash", generate_hash(attribute_columns, excluded_columns=excluded_attribute_from_hash))
        )
    elif write_mode == WriteMode.UPSERT:
        # Add UPSERT Technical Columns

        return (
            df.withColumn("_last_modified_date", F.current_timestamp())
            .withColumn("_hash", generate_hash(attribute_columns))
        )
    else:
        return df


def create_merge_condition(primary_keys, source="source", target="target", operator="AND", include_hash=False):
    """Generate merge condition for SCD2 merge operation.

    Args:
        primary_keys (list[str]): List of ID columns.
        source (str, optional): Alias for the source table. Defaults to "source".
        target (str, optional): Alias for the target table. Defaults to "target".
        operator (str, optional): Merge condition operator. Defaults to "AND".

    Returns:
        str: Merge condition string.
    """

    condition = f" {operator} ".join(
        [f"{source}.`{id_column}` = {target}.`{id_column}`" for id_column in primary_keys]
    )

    if include_hash:
        condition += " AND source._hash = target._hash"

    return condition


def create_hash_string_representation(columns, alias=None):

    sorted_columns = sorted(columns)

    if alias:
        casted_columns = ", ".join(f"COALESCE(CAST(`{alias}.{c}` AS STRING), '')" for c in sorted_columns)
    else:
        casted_columns = ", ".join(f"COALESCE(CAST(`{c}` AS STRING), '')" for c in sorted_columns)
    return f"MD5(CONCAT({casted_columns}))"


def create_concat_string_representation(columns, alias=None):

    sorted_columns = sorted(columns)

    if alias:
        casted_columns = ", ".join(f"COALESCE(CAST({alias}.`{c}` AS STRING), '')" for c in sorted_columns)
    else:
        casted_columns = ", ".join(f"COALESCE(CAST(`{c}` AS STRING), '')" for c in sorted_columns)
    return f"CONCAT({casted_columns})"


def merge_scd2(
    df: DataFrame,
    primary_keys: list[str],
    target_table_name: str,
    spark: SparkSession,
):
    """
    Perform a merge operation using SCD2 type 2 strategy.

    Args:
        df (DataFrame): Source DataFrame.
        primary_keys (list[str]): List of columns to use as merge keys.
        target_table_name (str): Name of the target table.
        spark (SparkSession): Spark session.

    Returns:
        None
    """

    target = spark.table(target_table_name).where("_is_active = true")

    join_condition = [F.col(f"updates.{c}") == F.col(f"target.{c}") for c in primary_keys]

    updates_of_existing_lines = (
        df.withColumn("_merge_key", F.lit(None))
        .alias("updates")
        .join(target.alias("target"), join_condition)
        .where("updates._hash != target._hash")
        .select("updates.*")
    )

    # Create temp view to serve as source for our SCD2 merge
    tmp_view_name = f"tmp_{target_table_name.replace('.', '_')}"

    (
        df.withColumn("_merge_key", generate_concat(primary_keys))
        .unionByName(updates_of_existing_lines)
        .createOrReplaceTempView(f"{tmp_view_name}")
    )

    logger.info(f"Merging into table {target_table_name}")
    merge_query = f"""
        MERGE INTO {target_table_name} AS target
        USING {tmp_view_name} AS source
        ON source._merge_key = {create_concat_string_representation(primary_keys, alias='target')}
        WHEN MATCHED AND (
            target._is_active = TRUE AND
            target._hash != source._hash
        )
        THEN UPDATE SET
            target._end_date = current_timestamp(),
            target._is_active = FALSE
        WHEN NOT MATCHED
            THEN INSERT *
    """
    logger.info(f"Merge query : {merge_query}")
    spark.sql(merge_query)
    logger.info(f"Successfully merged into table {target_table_name}")


def merge_upsert(
    df: DataFrame,
    primary_keys: list[str],
    target_table_name: str,
    spark: SparkSession,
):
    """
    Perform a merge operation using UPSERT strategy.

    Args:
        df (DataFrame): Source DataFrame.
        primary_keys (list[str]): List of columns to use as merge keys.
        target_table_name (str): Name of the target table.
        spark (SparkSession): Spark session.

    Returns:
        None
    """

    # Create temp view to serve as source for our SCD2 merge
    tmp_view_name = f"tmp_{target_table_name.replace('.', '_')}"

    # Create temp view for merge query
    df.createOrReplaceTempView(f"{tmp_view_name}")

    logger.info(f"Upserting into table {target_table_name}")

    # get target table and alter target table in case of new columns added
    target = spark.table(target_table_name)

    new_columns = [c for c in df.columns if c not in target.columns]

    # alter target table to add new columns
    if new_columns:
        for c in new_columns:
            spark.sql(f"ALTER TABLE {target_table_name} ADD COLUMN `{c}` STRING")

    # Prepare upsert query string
    # updates_columns = """
    #         , """.join(f"target.`{c}` = source.`{c}`" for c in df.columns)

    upsert_query = f"""
        MERGE INTO {target_table_name} AS target
        USING {tmp_view_name} AS source
        ON {create_merge_condition(primary_keys)}
        WHEN MATCHED AND target._hash != source._hash
            THEN UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """
    logger.info(f"Upsert query : {upsert_query}")
    spark.sql(upsert_query)
    logger.info(f"Successfully upserted into table {target_table_name}")


def write_iceberg(
    df: DataFrame,  # source dataframe
    table_name: str,  # Name of the target iceberg table
    glue_context: GlueContext,  # The glue context variable
    spark: SparkSession,  # The spark session
    write_mode: str = WriteMode.SCD2,  # Mode can only take options : scd2, append or overwrite or overwritePartitions or upsert
    primary_keys: list[str] = [],  # list of primary keys
    partitionedBy: list[str] = [],
    orderedBy: list[str] = [],
    options: dict = {},
    format_version: str = "2",
    tableProperties: dict = {},
    excluded_attribute_from_hash: list[str] = [],
    datetime_column: str = None,
):
    """
    Write to iceberg table using SCD2

    Args:
        df (DataFrame): source dataframe
        table_name (str): Name of the target iceberg table
        glue_context (GlueContext): The glue context variable
        spark (SparkSession): The spark session
        write_mode (str): Iceberg Write Mode: Options : scd2, append or overwrite or overwritePartitions or upsert
        primary_keys list[str]: list of primary keys, mandatory with SCD2 and UPSERT
        partitionedBy: list[Column | str] = [],  # list of columns to partition by
        orderedBy: list[str] = [],  # list of columns to order by
        options: dict = {},  # Options to pass to the write operation
        format_version: str = "2",  # Iceberg format version
        tableProperties: dict = {},  # Properties to set on the table

    Returns:
        None

    Example:
    table_tools.write_iceberg(
        df=df,
        table_name="ems_entitlementusage",
        glue_context=gc,
        spark=spark,
        write_mode=table_tools.WriteMode.OVERWRITE_PARTITIONS,
        partitionedBy=["customer_id", "entitlement_id", "product_key"],
        orderedBy=["customer_id", "entitlement_id", "product_key"],
        tableProperties={
            "write.target-file-size-bytes": "134217728",
            "write.parquet.row-group-size-bytes": "16777216"
        }
    )

    """

    # Remove perfect duplicates for safety of the merge
    df = df.drop_duplicates()

    if write_mode in (WriteMode.SCD2, WriteMode.UPSERT) and primary_keys:

        has_duplicates = check_duplicates(df, primary_keys, table_name)

        if has_duplicates:
            if datetime_column and primary_keys:
                logger.info(f"Detected datetime_column {datetime_column} and primary_keys {primary_keys}.")
                logger.info("Trying to remove duplicates with row_number operation...")

                window = Window.partitionBy(primary_keys).orderBy(F.col(datetime_column).desc())
                df = df.withColumn("row_number", F.row_number().over(window))
                df = df.filter(F.col("row_number") == 1).drop("row_number")
            else:
                raise has_duplicates

    # Peform Table Write
    catalog = spark.conf.get("spark.environment.catalog_name")
    database = spark.conf.get("spark.environment.database_name")
    env = spark.conf.get("spark.environment.env")
    s3_path = spark.conf.get("spark.environment.data_s3_path")
    db = spark.conf.get("spark.environment.db")
    target_table_name = f"{catalog}.{database}.{table_name}"

    # Check if table exists
    table_exists = etl_tools.check_table_exists(
        gc=glue_context,
        env=env,
        db=db,
        table=table_name,
    )

    # Add technical columns
    df = add_tech_columns(df, primary_keys, write_mode, excluded_attribute_from_hash)

    # Set ordering
    if orderedBy:
        df = df.orderBy(*orderedBy)

    # Get the global writer object
    dataFrameWriter = df.writeTo(target_table_name).using("iceberg").tableProperty("format-version", format_version)

    # Set partitionning
    if partitionedBy:
        partion_columns = [c if isinstance(c, Column) else F.col(c) for c in partitionedBy]
        dataFrameWriter = dataFrameWriter.partitionedBy(*partion_columns)

    # Set properties
    if tableProperties:
        for key, value in tableProperties.items():
            dataFrameWriter = dataFrameWriter.tableProperty(key, value)

    # Set options
    if options:
        dataFrameWriter = dataFrameWriter.options(**options)

    if table_exists:
        logger.info(f"{table_exists=} Table {target_table_name} exists at {s3_path}!")

        if write_mode == WriteMode.APPEND:
            # APPEND
            dataFrameWriter.append()
            logger.info(f"Successfully done append of table {table_name}")

        elif write_mode == WriteMode.OVERWRITE_PARTITIONS:
            # OVERWRITE TABLE PARTITIONS (PRESERVE EXISTING PARTITIONS)
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            dataFrameWriter.overwritePartitions()
            logger.info(f"Successfully done overwite partitions of table {table_name}")

        elif write_mode == WriteMode.OVERWRITE:
            # OVERWRITE TABLE (ERASE EXISTING PARTITIONS)
            dataFrameWriter.replace()
            logger.info(f"Successfully done overwite of table {table_name}")

        elif write_mode == WriteMode.UPSERT:
            # UPSERT INTO TABLE
            merge_upsert(
                df=df,
                primary_keys=primary_keys,
                target_table_name=target_table_name,
                spark=spark,
            )

        elif write_mode == WriteMode.SCD2:
            # MERGE INTO TABLE
            merge_scd2(
                df=df,
                primary_keys=primary_keys,
                target_table_name=target_table_name,
                spark=spark,
            )
        else:
            raise ValueError(f"""<write_mode> only takes : {' or '.join(WriteMode.get_all())} !
                             '{write_mode}' was given instead.""")

    else:
        # CREATE TABLE
        logger.info(f"{table_exists=} Table {target_table_name} does not exist at {s3_path}!")
        logger.info(f"Creating table {target_table_name}")

        dataFrameWriter.create()

        logger.info(f"Successfully created table {target_table_name}")


class TableMetadata:

    def __init__(self, spark, catalog, database, table):
        self.spark = spark
        self.catalog = catalog
        self.database = database
        self.table = table

    GetLiteralOptions = Literal[
        "snapshots",
        "history",
        "manifests",
        "files",
        "partitions",
        "entries",
        "metadata_log_entries",
        "position_deletes",
        "all_data_files",
        "all_delete_files",
        "all_entries",
        "all_manifests",
        "refs"
    ]

    def get(self, info: GetLiteralOptions, columns="*"):
        """
        info: str - Options: snapshots, history, manifests, files, partitions, entries, metadata_log_entries, position_deletes, all_data_files, all_delete_files, all_entries, all_manifests, refs
        """

        query = f"SELECT {columns} FROM {self.catalog}.{self.database}.{self.table}.{info}"

        return self.spark.sql(query)

    CallLiteralOptions = Literal[
        "rollback_to_snapshot",
        "rollback_to_timestamp",
        "set_current_snapshot",
        "cherrypick_snapshot",
        "publish_changes",
        "fast_forward",
        "expire_snapshots",
        "remove_orphan_files",
        "rewrite_data_files",
        "rewrite_manifests",
        "rewrite_position_delete_files",
        "snapshot",
        "migrate",
        "add_files",
        "register_table",
        "ancestors_of",
        "create_changelog_view",
        "compute_table_stats"
    ]

    def call(self, method: CallLiteralOptions, options=None, procedure_db="system"):
        if options:
            arguments = ", ".join([f"""
                {k} => {v}""" for k, v in options.items()])
        else:
            arguments = ""
        separator = "," if arguments else ""
        procedure = f"""
            CALL {self.catalog}.{procedure_db}.{method}(
                table => '{self.database}.{self.table}'{separator}
                {arguments}
            )
        """
        print(procedure)
        return self.spark.sql(procedure)


def deactivate_deleted_records(
    target_df: DataFrame,
    datasource,
    spark: SparkSession,
    deleted_primary_keys: list[str],
):
    """Mark deleted records in the target table based on the primary keys."""

    catalog = spark.conf.get("spark.environment.catalog_name")
    database = spark.conf.get("spark.environment.database_name")

    logger.info(f"Marking deleted records in target table {datasource.target_table_name}...")
    if "_is_deleted" not in target_df.columns:
        # Add '_is_deleted' column if it does not exist
        logger.info("'_is_deleted' non existent, adding it to the target table...")
        create_is_deleted_column_query = f"""
            ALTER TABLE {catalog}.{database}.{datasource.target_table_name}
                ADD COLUMN _is_deleted BOOLEAN
        """
        logger.info(f"Executing query to add '_is_deleted' column: {create_is_deleted_column_query}")
        spark.sql(create_is_deleted_column_query)

        # Initialize '_is_deleted' column to FALSE for all rows
        initialize_is_deleted_query = f"""
            UPDATE {catalog}.{database}.{datasource.target_table_name}
            SET _is_deleted = FALSE
        """
        logger.info(f"Executing query to initialize '_is_deleted' column: {initialize_is_deleted_query}")
        spark.sql(initialize_is_deleted_query)

    # Create a DataFrame with the deleted primary keys
    deleted_rows_df = target_df.where(F.col(datasource.primary_keys[0]).isin(list(deleted_primary_keys)))
    deleted_rows_df = deleted_rows_df.withColumn("_is_deleted", F.lit(True))

    # If write_mode is SCD2, set _is_active to False for deleted rows
    if datasource.write_mode == WriteMode.SCD2:
        deleted_rows_df = deleted_rows_df.withColumn("_is_active", F.lit(False))
        deactivate_scd2 = ", _is_active = FALSE"
    else:
        deactivate_scd2 = ""

    # Create a temporary view for the deleted rows
    tmp_view_name = "deleted_rows_temp_view"
    deleted_rows_df.createOrReplaceTempView(tmp_view_name)

    # Merge the deleted rows into the target table
    number_of_deleted_primary_keys = len(deleted_primary_keys)

    logger.info(f"Marking {number_of_deleted_primary_keys} rows in target table {datasource.target_table_name} as deleted...")
    merge_query = f"""
        MERGE INTO {catalog}.{database}.{datasource.target_table_name} AS target
        USING {tmp_view_name} AS source
        ON {create_merge_condition(datasource.primary_keys)}
        WHEN MATCHED THEN
            UPDATE SET
                _is_deleted = TRUE
                {deactivate_scd2}
    """

    print(f"Executing delete query: {merge_query}")
    spark.sql(merge_query)
