#!/bin/env python
from pyspark.sql import SparkSession
import datetime
import pyspark.sql.types as T 
import pyspark.sql.functions as F 
from pyspark.storagelevel import StorageLevel
import argparse 
from dateutil import parser

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--raw-data", "-r",
        dest="raw_data",
        required=True,
        help="Pass the absolute path to the s3 raw data location until year partition",
        type=str
    )
    parser.add_argument(
        "--start-date-hour", "-s",
        dest="start_date",
        required=True,
        help="Pass the start date in YYYY-MM-DD HH format",
        type=str
    )
    parser.add_argument(
        "--end-date-hour", "-e",
        dest="end_date",
        required=True,
        help="Pass the end date in YYYY-MM-DD HH format",
        type=str
    )
    args = parser.parse_args()
    return args

def get_file_paths(base_path: str, year: int, month: int, day_start: int, day_end: int, hour_start: int, hour_end: int):
    try:
        paths = []

        for day in range(day_start, day_end+1):
            for hour in range(hour_start, hour_end):
                path = f"{base_path}year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}"
                paths.append(path)
        print(paths)
        return paths
    
    except Exception as e:
        raise e

# TODO USE dataclasses instead of dictionary
def get_tables(raw_data_path: str) -> tuple:
    catalog = "fetch_dl"
    db = "offers_prod"
    raw_data_origin = 'legacy'
    raw_offer_elig = f"{catalog}.{db}.raw_offer_eligibility_snapshots"
    stg_offer_elig = f"{catalog}.{db}.stg_offer_eligibility_snapshots"
    cs_active = f"{catalog}.{db}.offer_eligibility_snapshots_current_state_active"
    cs_inactive = f"{catalog}.{db}.offer_eligibility_snapshots_current_state_inactive"
    saved_offers = f"{catalog}.{db}.saved_offers"    
    if "kafka-stream" in raw_data_path:
        raw_offer_elig = f"{raw_offer_elig}_kafka"
        stg_offer_elig = f"{stg_offer_elig}_kafka"
        cs_active = f"{cs_active}_kafka"
        cs_inactive = f"{cs_inactive}_kafka"
        saved_offers = f"{saved_offers}_kafka"
        raw_data_origin = 'kafka'
    return {
        'catalog': catalog,
        'db': db,
        'raw': raw_offer_elig,
        'stg': stg_offer_elig,
        'cs_active': cs_active,
        'cs_inactive': cs_inactive,
        'saved_offers': saved_offers,
        'raw_data_origin': raw_data_origin
    }

def create_table(spark, tablename, df, partition_columns=None):
    schema = df.schema

    columns = []
    for field in schema.fields:
        field_name = field.name
        datatype = field.dataType.simpleString()
        columns.append(f"    {field_name} {datatype}")

    columns = ',\n'.join(columns)

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {tablename} (
            {columns}
        ) USING ICEBERG 
    """

    if partition_columns:
        partition_columns = ',\n'.join(partition_columns)
        create_table_sql += f"""
        PARTITIONED BY (
            {partition_columns}
        )
        """
    print(create_table_sql)
    spark.sql(create_table_sql)

def main(spark, args):

    base_path = args.raw_data
    start_date = parser.parse(args.start_date)
    end_date = parser.parse(args.end_date)

    catalog_tables = get_tables(base_path)
    raw_data_origin = catalog_tables['raw_data_origin']
    catalog = catalog_tables['catalog']
    db = catalog_tables['db']
    raw_offer_elig = catalog_tables['raw']
    stg_offer_elig = catalog_tables['stg']
    cs_active = catalog_tables['cs_active']
    cs_inactive = catalog_tables['cs_inactive']
    saved_offers = catalog_tables['saved_offers']

    # Read in incremental data
    # TODO: Build a metadata layer to handle last processed dates and desired wait time
    #       Refactor for a better date calculation logic
    paths = get_file_paths(
        year=start_date.year,
        month=start_date.month,
        day_start=start_date.day,
        hour_start=start_date.hour,
        day_end=end_date.day,
        hour_end=end_date.hour,
        base_path=base_path
    )

    df = spark.read.option("basePath", base_path)\
                .json(paths)
    df = df.withColumn("file_path", F.input_file_name())
    df = df.withColumn("year", F.regexp_extract("file_path", r"year=(\d{4})", 1).cast("int")) \
        .withColumn("month", F.regexp_extract("file_path", r"month=(\d{2})", 1).cast("int")) \
        .withColumn("day", F.regexp_extract("file_path", r"day=(\d{2})", 1).cast("int")) \
        .withColumn("hour", F.regexp_extract("file_path", r"hour=(\d{2})", 1).cast("int"))

    # Load incremental data to raw table 
    # spark.sql(f'drop table {raw_offer_elig}')
    create_table(spark=spark, tablename=raw_offer_elig, df=df, partition_columns=["year", "month", "day", "hour"])
    df.write.format("iceberg")\
        .mode("overwrite")\
        .partitionBy("year", "month", "day", "hour")\
        .option("partitionOverwriteMode", "dynamic")\
        .save(raw_offer_elig)

    # Get current anc historic batchid
    # TODO UPDATE Batchid to match with end time of run window
    # eg: if end time is 2024/12/12 Hour 03, batchid = 2024121203
    batch_id = datetime.datetime.now().strftime("%Y%m%d%H%M")
    hist_batch_id = spark.sql(f"SELECT MAX(batch_id) FROM {cs_active}")
    hist_batch_id = hist_batch_id.collect()[0][0]
    print(batch_id, hist_batch_id)


    # Set aliases and unpack progress
    if raw_data_origin == 'legacy':

        # STAGE Table with offers and progress data flattened and deduped
        df_stage = df.select('payload')
        df_stage = df_stage.withColumn("user_id", F.get_json_object(F.col("payload"), "$.userId")) \
            .withColumn("offers", F.get_json_object(F.col("payload"), "$.offers")) \
            .withColumn("updated_ts", F.get_json_object(F.col("payload"), "$.updated"))
        # Unpack Offers
        df_stage = df_stage.select(
                F.col("user_id"),
                F.explode(F.from_json(F.col("offers"), T.ArrayType(T.MapType(T.StringType(), T.StringType())))).alias("offers"),
                F.col("updated_ts")
        )

        df_stage = df_stage.select(
            F.col("user_id"),
            F.col("updated_ts"),
            F.col("offers.offerId").alias("offer_id"),
            F.col("offers.creationTime").alias("creation_time"),
            F.col("offers.audience").alias("audience"),
            F.col("offers.banditStatus").alias("bandit_status"),
            F.col("offers.experimentStatus").alias("experiment_status"),
            F.col("offers.holdout").alias("holdout"),
            F.col("offers.invalidationTime").alias("invalidation_time"),
            F.col("offers.startTime").alias("start_time"),
            F.col("offers.endTime").alias("end_time"),
            F.col("offers.potentiallyEligibleInFuture").alias("potentially_eligible_in_future"),
            F.col("offers.progress").alias("progress"),
            F.from_json(F.col("offers.progress").alias("progress"), T.MapType(T.StringType(), T.StringType())).alias('progress_parsed')
        )
        # Standardize timestamps and unpack progress
        df_stage = df_stage.select(
            F.col("user_id"),
            F.from_unixtime(F.col('updated_ts')/1000, "yyyy-MM-dd HH:mm:ss").alias('updated_ts'),
            F.col("offer_id"),
            F.from_unixtime(F.col('creation_time')/1000, "yyyy-MM-dd HH:mm:ss").alias('creation_time'),
            F.col("audience"),
            F.col("bandit_status"),
            F.col("experiment_status"),
            F.col("holdout"),
            F.from_unixtime(F.col('invalidation_time')/1000, "yyyy-MM-dd HH:mm:ss").alias('invalidation_time'),
            F.from_unixtime(F.col('start_time')/1000, "yyyy-MM-dd HH:mm:ss").alias('start_time'),
            F.from_unixtime(F.col('end_time')/1000, "yyyy-MM-dd HH:mm:ss").alias('end_time'),
            F.col("potentially_eligible_in_future"),
            F.col("progress_parsed.type").alias("progress_type"),
            F.col("progress_parsed.uses").alias("progress_uses"),
            F.col("progress_parsed.dollars").alias("progress_dollars"),
            F.col("progress_parsed.dollarsRequired").alias("progress_dollars_required"),
            F.col("progress_parsed.maxUses").alias("progress_max_uses"),
            F.col("progress_parsed.percentage").alias("progress_percentage"),
            F.col("progress_parsed.quantity").alias("progress_quantity"),
            F.col("progress_parsed.quantityRequired").alias("progress_quantity_required")
        )    
    elif raw_data_origin == 'kafka':
        # Unpack Offers
        df_stage = df.select(
            F.col("user_id"),
            F.explode(F.col("offers")).alias("offers"),
            F.col("updated_timestamp_ms").alias("updated_ts")
        )
        # KAFKA STREAMS DO NOT HAVE bandit status and experiment status
        # This block is just to handle that
        df_stage = df_stage.select(
            F.col("user_id"),
            F.col("updated_ts"),
            F.col("offers.offer_id").alias("offer_id"),
            F.col("offers.creation_timestamp_ms").alias("creation_time"),
            F.col("offers.audience").alias("audience"),
            F.lit("NULL").alias("bandit_status"), # Not present in Kafka Streams
            F.lit("NULL").alias("experiment_status"), # Not present in Kafka Streams
            F.col("offers.holdout").alias("holdout"),
            F.col("offers.invalidation_timestamp_ms").alias("invalidation_time"),
            F.col("offers.start_timestamp_ms").alias("start_time"),
            F.col("offers.end_timestamp_ms").alias("end_time"),
            F.col("offers.potentially_eligible_in_future").alias("potentially_eligible_in_future"),
            F.col("offers.progress").alias("progress_parsed"),
        )
        df_stage = df_stage.select(
            F.col("user_id"),
            F.from_unixtime(F.col('updated_ts')/1000, "yyyy-MM-dd HH:mm:ss").alias('updated_ts'),
            F.col("offer_id"),
            F.from_unixtime(F.col('creation_time')/1000, "yyyy-MM-dd HH:mm:ss").alias('creation_time'),
            F.col("audience"),
            F.col("bandit_status"),
            F.col("experiment_status"),
            F.col("holdout"),
            F.from_unixtime(F.col('invalidation_time')/1000, "yyyy-MM-dd HH:mm:ss").alias('invalidation_time'),
            F.from_unixtime(F.col('start_time')/1000, "yyyy-MM-dd HH:mm:ss").alias('start_time'),
            F.from_unixtime(F.col('end_time')/1000, "yyyy-MM-dd HH:mm:ss").alias('end_time'),
            F.col("potentially_eligible_in_future"),
            F.col("progress_parsed.type").alias("progress_type"),
            F.col("progress_parsed.uses").alias("progress_uses"),
            F.col("progress_parsed.dollars").alias("progress_dollars"),
            F.col("progress_parsed.dollars_required").alias("progress_dollars_required"),
            F.col("progress_parsed.max_uses").alias("progress_max_uses"),
            F.col("progress_parsed.percentage").alias("progress_percentage"),
            F.col("progress_parsed.quantity").alias("progress_quantity"),
            F.col("progress_parsed.quantity_required").alias("progress_quantity_required")
        )        


    # Dedupe staged data
    df_stage.createOrReplaceTempView("df_stage")
    df_stage_deduped = spark.sql(f"""
        WITH ranked AS (
            SELECT
                RANK() OVER (PARTITION BY user_id, offer_id ORDER BY updated_ts DESC) AS latest_ts_rank,
                user_id,
                updated_ts,
                offer_id,
                creation_time,
                audience,
                bandit_status,
                experiment_status,
                holdout,
                invalidation_time,
                start_time,
                end_time,
                potentially_eligible_in_future,
                progress_type,
                progress_uses,
                progress_dollars,
                progress_dollars_required,
                progress_max_uses,
                progress_percentage,
                progress_quantity,
                progress_quantity_required
        FROM df_stage
        )
        SELECT
                user_id,
                CAST(updated_ts AS timestamp) AS updated,
                offer_id,
                CAST(start_time AS timestamp) AS start_time,
                CAST(end_time AS timestamp) AS end_time,
                CAST(creation_time AS timestamp) AS creation_time,
                CAST(invalidation_time AS timestamp) AS invalidation_time,
                CAST(progress_dollars AS double) AS dollars,
                CAST(progress_percentage AS double) AS percentage,
                CAST(NVL(progress_quantity, 0) AS decimal(38,0)) AS quantity,
                CAST(holdout AS boolean) AS holdout,
                'NA' AS meta,
                0.0 AS ranking,
                CAST(Potentially_eligible_in_future AS boolean) AS potentiallyeligibleinfuture,
                CAST(audience AS boolean) AS audience,
                CAST(progress_uses AS decimal(38,0)) AS uses,
                bandit_status,
                experiment_status
        FROM ranked
        WHERE latest_ts_rank = 1

    """).persist(StorageLevel.MEMORY_ONLY)
    df_stage_deduped = df_stage_deduped.dropDuplicates()

    # Sometimes there are multiple entries for a user offer combination at same updated_ts
    # So far have only seen such cases where uses are different, in such cases retain highest uses
    df_stage_deduped.createOrReplaceTempView("df_stage_deduped")
    df_stage_deduped = spark.sql(f"""
    CREATE OR REPLACE TABLE {stg_offer_elig}
    USING ICEBERG
    PARTITIONED BY (offer_id)
    AS SELECT
                user_id,
                updated,
                offer_id,
                start_time,
                end_time,
                creation_time,
                invalidation_time,
                dollars,
                percentage,
                quantity,
                holdout,
                meta,
                ranking,
                potentiallyeligibleinfuture,
                audience,
                MAX(uses) AS uses,
                bandit_status,
                experiment_status
        FROM df_stage_deduped
        GROUP BY 
                user_id,
                updated,
                offer_id,
                start_time,
                end_time,
                creation_time,
                invalidation_time,
                dollars,
                percentage,
                quantity,
                holdout,
                meta,
                ranking,
                potentiallyeligibleinfuture,
                audience,
                bandit_status,
                experiment_status    
        """)

    # New Mappings
    incr_new_mappings = spark.sql(f"""
    WITH cs_active AS (
        SELECT offer_id, user_id 
        FROM {cs_active} hist
        WHERE batch_id = {hist_batch_id}
    )
    SELECT incr.*
    FROM {stg_offer_elig} incr 
    LEFT JOIN cs_active hist 
    ON (incr.offer_id = hist.offer_id AND incr.user_id = hist.user_id)
    WHERE hist.offer_id IS NULL
    """)

    incr_new_mappings_current = incr_new_mappings.select("*").where("potentiallyeligibleinfuture = 'false'")
    incr_new_mappings_current = incr_new_mappings_current.withColumn("start_time_first_observed", F.col("start_time"))
    incr_new_mappings_current = incr_new_mappings_current.withColumn("start_time_recently_observed", F.lit(None))
    incr_new_mappings_current = incr_new_mappings_current.withColumn("percentage_recently_observed", F.lit(None))
    incr_new_mappings_current = incr_new_mappings_current.withColumn("uses_recently_observed", F.lit(None))

    incr_new_mappings_future = incr_new_mappings.select("*").where("potentiallyeligibleinfuture = 'true'")
    incr_new_mappings_future = incr_new_mappings_future.withColumn("start_time_first_observed", F.lit(None))
    incr_new_mappings_future = incr_new_mappings_future.withColumn("start_time_recently_observed", F.lit(None))
    incr_new_mappings_future = incr_new_mappings_future.withColumn("percentage_recently_observed", F.lit(None))
    incr_new_mappings_future = incr_new_mappings_future.withColumn("uses_recently_observed", F.lit(None))

    incr_new_mappings_current_n_future = incr_new_mappings_current.union(incr_new_mappings_future)

    # Base Condition
    incr_matching_mappings = spark.sql(f"""
    WITH cs_active AS (
        SELECT 
              offer_id
            , user_id 
            , start_time_first_observed
            , start_time_recently_observed
            , percentage_recently_observed
            , uses_recently_observed
            , start_time
            , percentage
            , uses
            , updated
        FROM {cs_active}
        WHERE batch_id = {hist_batch_id}
    )
    SELECT incr.*
         , hist.start_time_first_observed
         , hist.start_time_recently_observed
         , hist.percentage_recently_observed
         , hist.uses_recently_observed
         , hist.start_time AS hist_start_time
         , hist.percentage AS hist_percentage
         , hist.uses AS hist_uses
    FROM {stg_offer_elig} incr 
    INNER JOIN cs_active hist 
    ON (incr.offer_id = hist.offer_id AND incr.user_id = hist.user_id)
    WHERE incr.updated > hist.updated 
    """)
    incr_matching_mappings = incr_matching_mappings.dropDuplicates()


    # Merge Condition 1 with Base Condition 

    incr_match_merge1 = incr_matching_mappings.select("*").where("start_time_first_observed IS NULL AND potentiallyeligibleinfuture = 'false'")
    incr_match_merge1 = incr_match_merge1.withColumn("start_time_first_observed", F.col("start_time"))


    # Merge Condition 2 with Base Condition 

    incr_match_merge2 = incr_matching_mappings.select("*").where("start_time_first_observed IS NOT NULL AND potentiallyeligibleinfuture = 'false' AND percentage IS NOT NULL")
    incr_match_merge2 = incr_match_merge2.withColumn("start_time_recently_observed", F.col("hist_start_time")) #should this be incr.start time?
    incr_match_merge2 = incr_match_merge2.withColumn("percentage_recently_observed", F.col("hist_percentage")) #should this be incr.percentage?
    incr_match_merge2 = incr_match_merge2.withColumn("uses_recently_observed", F.col("hist_uses"))

    # Merge Condition 3 with Base Condition 

    incr_match_merge3 = incr_matching_mappings.select("*").where("start_time_first_observed IS NOT NULL AND potentiallyeligibleinfuture = 'false' AND percentage IS NULL")
    incr_match_merge3 = incr_match_merge3.withColumn("start_time_recently_observed", F.col("hist_start_time"))
    incr_match_merge3 = incr_match_merge3.withColumn("uses_recently_observed", F.col("hist_uses"))

    # Merge Condition 3 with Base Condition 

    incr_match_merge4 = incr_matching_mappings.select("*").where("start_time_first_observed IS NULL AND potentiallyeligibleinfuture = 'true'")

    incr_match_merged = incr_match_merge1.union(incr_match_merge2).union(incr_match_merge3).union(incr_match_merge4)
    incr_match_merged = incr_match_merged.drop('hist_start_time', 'hist_percentage', 'hist_uses')

    df_for_merge = incr_match_merged.union(incr_new_mappings_current_n_future)
    df_for_merge = df_for_merge.withColumn("expiry_status", F.lit('active'))
    df_for_merge = df_for_merge.withColumn("batch_id", F.lit(batch_id))

    spark.sql(f"ALTER TABLE {cs_active} DROP BRANCH IF EXISTS incr_branch")
    spark.sql(f"ALTER TABLE {cs_active} CREATE BRANCH IF NOT EXISTS incr_branch")
    spark.sql("SET spark.wap.branch = incr_branch")

    df_for_merge.createOrReplaceTempView('incr_elig_updates')
    spark.sql(f"""INSERT INTO {cs_active}
        SELECT  
              USER_ID
            , UPDATED
            , offer_id
            , START_TIME
            , END_TIME
            , CREATION_TIME
            , INVALIDATION_TIME
            , DOLLARS
            , PERCENTAGE
            , QUANTITY
            , HOLDOUT
            , META
            , RANKING
            , POTENTIALLYELIGIBLEINFUTURE
            , START_TIME_FIRST_OBSERVED
            , START_TIME_RECENTLY_OBSERVED
            , AUDIENCE
            , PERCENTAGE_RECENTLY_OBSERVED
            , USES
            , USES_RECENTLY_OBSERVED
            , BANDIT_STATUS
            , EXPERIMENT_STATUS
            , expiry_status
            , CAST(batch_id AS BIGINT) AS batch_id
        FROM incr_elig_updates"""
    )

    spark.sql(f"""
    WITH cs_active AS (
        SELECT *
        FROM {cs_active}
        WHERE batch_id = {hist_batch_id}
    ),
    incr_elig_updates_ids AS (
        SELECT DISTINCT user_id, offer_id 
        FROM incr_elig_updates
    ),
    cs_active_include AS (
        SELECT hist.*
        FROM cs_active hist 
        LEFT JOIN incr_elig_updates_ids incr ON (hist.offer_id = incr.offer_id AND hist.user_id = incr.offer_id)
        WHERE incr.offer_id IS NULL
    )
    INSERT INTO {cs_active}
        SELECT  
              USER_ID
            , UPDATED
            , offer_id
            , START_TIME
            , END_TIME
            , CREATION_TIME
            , INVALIDATION_TIME
            , DOLLARS
            , PERCENTAGE
            , QUANTITY
            , HOLDOUT
            , META
            , RANKING
            , POTENTIALLYELIGIBLEINFUTURE
            , START_TIME_FIRST_OBSERVED
            , START_TIME_RECENTLY_OBSERVED
            , AUDIENCE
            , PERCENTAGE_RECENTLY_OBSERVED
            , USES
            , USES_RECENTLY_OBSERVED
            , BANDIT_STATUS
            , EXPERIMENT_STATUS
            , expiry_status
            , CAST({batch_id} AS BIGINT) AS batch_id
        FROM cs_active_include
    """
    )

    spark.sql(f"CALL fetch_dl.system.fast_forward('{cs_active}', 'main', 'incr_branch')")
    spark.sql(f"alter table {cs_active} drop branch incr_branch")
    spark.sql(f"CALL fetch_dl.system.remove_orphan_files(table => '{cs_active}')")
    spark.sql(f"CALL fetch_dl.system.rewrite_data_files(table => '{cs_active}', where => 'batch_id={batch_id}', options => map('max-concurrent-file-group-rewrites', '1000'))")
    spark.sql(f"CALL fetch_dl.system.rewrite_manifests(table => '{cs_active}')")
    spark.sql(f"CALL fetch_dl.system.rewrite_position_delete_files(table => '{cs_active}', where => 'batch_id={batch_id}', options => map('max-concurrent-file-group-rewrites', '1000'))")

    # Stop Spark session
    spark.stop()
    return True


if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("offer-eligibility") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.fetch_dl", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.fetch_dl.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.fetch_dl.warehouse", "s3://data-lake-094506541504-us-east-1-preprod/dev/fetch-dl/warehouse/") \
        .config("spark.sql.catalog.fetch_dl.glue.database", "offers_prod") \
        .config("spark.sql.catalog.fetch_dl.glue.region", "us-east-1") \
        .config("spark.sql.files.maxPartitionBytes", "536870912") \
        .getOrCreate()
    
    args = get_args()
        

    main(spark, args)
