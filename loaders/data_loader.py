import os
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timedelta
import traceback
from pyspark.sql.functions import col
from utils.constants import AUTO_LOCAL_IDS, CANCELLATION_IDS, GREYSTAR_IDS

def load_data(spark: SparkSession, path: str):
    return spark.read.csv(path, header=True, inferSchema=True)

def load_events(start_date: datetime, end_date: datetime, spark: SparkSession):
    """
    Load events data from BigQuery.
    """
    suffix = []
    current = start_date
    res: DataFrame =  None
    while current <= end_date:
        cur_suffix = current.strftime("%Y%m%d")
        try:
            df = spark.read.format("com.google.cloud.spark.bigquery") \
                .option("table", f"clx-ga4.ga4_flat.events_{cur_suffix}") \
                .option("location", "US") \
                .load()
            res = res.unionByName(df) if res else df
            current += timedelta(days=1)
        except:
            traceback.print_exc()
            current += timedelta(days=1)
    return res

def load_tcc_companies(spark: SparkSession):
    """
    Load TCC companies data from BigQuery.
    """
    
    query = """
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY name ORDER BY master_id DESC) as rn
        FROM `xperience-prod.tcc.companies`
        WHERE active = TRUE
        AND master_id IS NOT NULL
    )
    WHERE rn = 1
    """

    df = spark.read.format("com.google.cloud.spark.bigquery") \
    .option("query", query) \
    .option("parentProject", "xperience-prod") \
    .option("materializationDataset", "tcc") \
    .option("viewsEnabled", "true") \
    .load()

    combined_ids = CANCELLATION_IDS + GREYSTAR_IDS + AUTO_LOCAL_IDS

    df = df.filter(~col("master_id").isin(combined_ids))

    return df
        

def load_tcc_lead_journeys(
    start_date: datetime,
    end_date: datetime,
    spark: SparkSession
):
    df = spark.read.format("com.google.cloud.spark.bigquery") \
        .option("table", "xperience-prod.tcc.lead_journeys") \
        .option("location", "us-central1") \
        .option("filter", f"created_at >= '{start_date.strftime('%Y-%m-%d')}' AND created_at <= '{end_date.strftime('%Y-%m-%d')}'") \
        .load()
    
    combined_ids = CANCELLATION_IDS + GREYSTAR_IDS + AUTO_LOCAL_IDS

    df = df.filter(~col("odoo_master_id").isin(combined_ids))

    return df

def load_tcc_sessions(
    start_date: datetime,
    end_date: datetime,
    spark: SparkSession
):
    df = spark.read.format("com.google.cloud.spark.bigquery") \
        .option("table", "xperience-prod.tcc.widget_metric_sessions") \
        .option("location", "us-central1") \
        .load() \
        .select("id", "created_at", "machine_guid", "region", "city", "ip", "session_guid", "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "company_id") \
        .filter(f"created_at >= '{start_date.strftime('%Y-%m-%d')}' AND created_at <= '{end_date.strftime('%Y-%m-%d')}'")
    
    return df

def load_odoo_location(spark: SparkSession):
    """
    Load and deduplicate Odoo location data from BigQuery.
    
    This function loads data from the odoo_locations table, filtering out null master_ids
    and removing duplicates based on the 'name' column, keeping the entry with the
    highest 'master_id'.
    """
    
    query = """
        SELECT *
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY name ORDER BY master_id DESC) as rn
            FROM `clx-ga4.ga4_flat.odoo_locations`
            WHERE master_id IS NOT NULL
        )
        WHERE rn = 1
    """

    df = spark.read.format("com.google.cloud.spark.bigquery") \
        .option("query", query) \
        .option("parentProject", "clx-ga4") \
        .option("materializationDataset", "ga4_flat") \
        .option("viewsEnabled", "true") \
        .load()
    
    combined_ids = AUTO_LOCAL_IDS + CANCELLATION_IDS + GREYSTAR_IDS
    
    df = df.filter(~col("master_id").isin(combined_ids))
    
    return df
    



