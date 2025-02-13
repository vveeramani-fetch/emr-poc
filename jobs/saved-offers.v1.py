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
    raw = f"{catalog}.{db}.raw_saved_offers"
    stg = f"{catalog}.{db}.stg_saved_offers"
    saved_offers = f"{catalog}.{db}.saved_offers"    
    return {
        'catalog': catalog,
        'db': db,
        'raw': raw,
        'stg': stg,
        'saved_offers': saved_offers
    }

def main(spark, args):

    base_path = args.raw_data
    start_date = parser.parse(args.start_date)
    end_date = parser.parse(args.end_date)

    catalog_tables = get_tables(base_path)
    catalog = catalog_tables['catalog']
    db = catalog_tables['db']
    raw_saved_offers = catalog_tables['raw']
    stg_saved_offers = catalog_tables['stg']
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
    df = df.sortWithinPartitions("year", "month", "day", "hour")
    df = df.select('id', 
                'eligibilityRequirement.latestEndTime',
                'startTime',
                'endTime', 
                'year', 
                'month', 
                'day', 
                'hour'
        )

    df.write.format("iceberg")\
        .mode("overwrite")\
        .option("overwrite-mode", "dynamic")\
        .insertInto(raw_saved_offers)

    # Incremental Stage Load
    spark.sql(f"""
    CREATE OR REPLACE TABLE {stg_saved_offers}
    USING ICEBERG 
    AS 
    WITH stg AS (
        SELECT
            id AS offer_id
            , FROM_UTC_TIMESTAMP(FROM_UNIXTIME(startTime/1000), 'America/Chicago') AS start_time
            , FROM_UTC_TIMESTAMP(FROM_UNIXTIME(endTime/1000), 'America/Chicago') AS end_time
            , FROM_UTC_TIMESTAMP(FROM_UNIXTIME(latestEndTime/1000), 'America/Chicago') AS latest_end_time
            , year
            , month
            , day
            , hour
            , DENSE_RANK() OVER(PARTITION BY id ORDER BY MAKE_TIMESTAMP(year, month, day, hour, 0, 0) DESC) AS latest_updated_ts_rank
        FROM {raw_saved_offers}
        WHERE startTime+endTime > 0
    ),
    latest_update AS (
        SELECT DISTINCT
            offer_id
            , start_time
            , end_time
            , latest_end_time
            , year
            , month
            , day
            , hour
        FROM stg
        WHERE latest_updated_ts_rank = 1
    ),
    deduped_latest_update AS (
        SELECT 
            offer_id
            , MAX(start_time) AS start_time
            , MAX(end_time) AS end_time
            , MAX(latest_end_time) AS latest_end_time
            , year
            , month
            , day
            , MAX(hour) AS hour
        FROM latest_update 
        GROUP BY 
            offer_id
            , year
            , month
            , day
    )
    SELECT *
    FROM deduped_latest_update 
    """)

    # Merge to Final saved_offers
    spark.sql(f"""
        MERGE INTO {saved_offers} AS t
        USING {stg_saved_offers} AS s
        ON (t.offer_id = s.offer_id)
        WHEN MATCHED 
        AND s.start_time != t.start_time OR s.end_time != t.end_time OR s.latest_end_time != t.latest_end_time
        THEN UPDATE SET *
        WHEN NOT MATCHED
        THEN INSERT *
    """)

    # Stop Spark session
    spark.stop()
    return True


if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("saved-offers") \
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
