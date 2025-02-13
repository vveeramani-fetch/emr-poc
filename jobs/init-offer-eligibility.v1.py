#!/bin/env python
# Applicable only to the legacy current state

from pyspark.sql import SparkSession
import datetime
import pyspark.sql.types as T 
import pyspark.sql.functions as F 
from pyspark.storagelevel import StorageLevel
import argparse 
from dateutil import parser
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
log_format = '%(asctime)s [%(levelname)s] %(funcName)s: %(message)s'
date_format = '%Y-%m-%d %H:%M:%S' 
console_handler = logging.StreamHandler()
formatter = logging.Formatter(log_format, datefmt=date_format)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--raw-data", "-r",
        dest="raw_data",
        required=True,
        help="Pass the absolute path to the s3 raw data location until date partition",
        type=str
    )
    args = parser.parse_args()
    return args

# TODO USE dataclasses instead of dictionary
def get_tables(raw_data_path: str, pipetype: str) -> tuple:
    catalog = "fetch_dl"
    db = "offers_prod"
    sf_cs_dump = f"{catalog}.{db}.sf_offer_eligibility_cs_dump"
    raw_offer_elig = f"{catalog}.{db}.raw_offer_eligibility_snapshots"
    stg_offer_elig = f"{catalog}.{db}.stg_offer_eligibility_snapshots"
    int_cs_w_expiry = f"{catalog}.{db}.sf_offer_eligibility_cs_dump_w_expiry"
    cs_active = f"{catalog}.{db}.offer_eligibility_snapshots_current_state_active"
    cs_inactive = f"{catalog}.{db}.offer_eligibility_snapshots_current_state_inactive"
    saved_offers = f"{catalog}.{db}.saved_offers"    
    return {
        'catalog': catalog,
        'db': db,
        'sf_dump': sf_cs_dump,
        'raw': raw_offer_elig,
        'stg': stg_offer_elig,
        'int': int_cs_w_expiry,
        'cs_active': cs_active,
        'cs_inactive': cs_inactive,
        'saved_offers': saved_offers,
        'raw_data_origin': raw_data_origin
    }

def main(spark, args):

    base_path = args.raw_data
    pipe_type = args.pipe_type
    batch_id = base_path.split('/')[-1].split('=')[1]

    catalog_tables = get_tables(base_path, pipe_type)
    raw_data_origin = catalog_tables['raw_data_origin']
    catalog = catalog_tables['catalog']
    db = catalog_tables['db']
    sf_dump = catalog_tables['sf_dump']
    sf_dump_int = catalog_tables['int']
    raw_offer_elig = catalog_tables['raw']
    stg_offer_elig = catalog_tables['stg']
    cs_active = catalog_tables['cs_active']
    cs_inactive = catalog_tables['cs_inactive']
    saved_offers = catalog_tables['saved_offers']

    logger.info(f'Writing to Raw Iceberg Table: {sf_dump}')
    # write to iceberg table
    df=spark.read.parquet(base_path)
    df.write.format("iceberg").mode('overwrite').save(sf_dump)

    logger.info(f'Setting up table with expiry: {sf_dump_int}')
    # Create Table with expiry
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {sf_dump_int}
        USING ICEBERG
        PARTITIONED BY (expiry_status)
        AS 
        SELECT 
            t1.*, 
            CASE WHEN DATE_ADD(t2.latest_end_time, 91) < CURRENT_DATE() THEN "expired" ELSE "active" END AS expiry_status 
        FROM {sf_dump} t1
        LEFT JOIN {saved_offers} t2 ON (t1.offer_id = t2.offer_id)
    """)

    logger.info(f'Setting up Active CS: {cs_active}')
    # Create Active Table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cs_active} 
        USING ICEBERG 
        PARTITIONED BY (batch_id, offer_id)
        TBLPROPERTIES (
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read'
        )        
        AS SELECT 
            * 
            , CAST({batchid} AS BIGINT) AS batch_id
        FROM {sf_dump_int}
        WHERE expiry_status != 'expired'
    """)

    logger.info(f'Setting up Expired CS: {cs_inactive}')
    # Create Inactive Table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cs_inactive} 
        USING ICEBERG 
        PARTITIONED BY (batch_id, offer_id)
        TBLPROPERTIES (
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read'
        )        
        AS SELECT 
            * 
            , CAST({batchid} AS BIGINT) AS batch_id
        FROM {sf_dump_int}
        WHERE expiry_status = 'expired'
    """)

    logger.info(f'Closing Session')
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
