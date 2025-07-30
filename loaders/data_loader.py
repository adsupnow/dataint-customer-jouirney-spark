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
    

def load_backfilled_companies(spark: SparkSession, tcc_companies_df: DataFrame) -> DataFrame:
    """
    Fetches active companies from BigQuery with missing master_id,
    backfills them using odoo_locations, excluding companies already
    present in the input tcc_companies_df.

    Args:
        spark: The SparkSession object.
        tcc_companies_df: DataFrame of TCC companies that have already been loaded.

    Returns:
        A DataFrame containing ONLY the companies that were
        successfully backfilled with a new master_id.
    """
    # Step 1: Read active TCC companies from BigQuery that have a null master_id.
    companies_to_fix_from_bq_df = spark.read.format("com.google.cloud.spark.bigquery") \
        .option("table", "xperience-prod.tcc.companies") \
        .option("location", "us-central1") \
        .option("filter", "active = TRUE AND master_id IS NULL") \
        .load()

    # Step 2: Read deduplicated Odoo locations to be used for mapping.
    # The query ensures we get a unique name-to-master_id mapping.
    odoo_locations_query = """
        SELECT name, master_id
        FROM (
            SELECT name, master_id, ROW_NUMBER() OVER (PARTITION BY name ORDER BY master_id DESC) as rn
            FROM `clx-ga4.ga4_flat.odoo_locations`
            WHERE master_id IS NOT NULL AND name IS NOT NULL
        )
        WHERE rn = 1
    """
    dedup_odoo_locations_df = spark.read.format("com.google.cloud.spark.bigquery") \
        .option("query", odoo_locations_query) \
        .option("parentProject", "clx-ga4") \
        .option("materializationDataset", "ga4_flat") \
        .option("viewsEnabled", "true") \
        .load() \
        .withColumnRenamed("master_id", "new_master_id")

    # Step 3: Find companies that need backfilling by excluding those already present
    # in the input tcc_companies_df.
    companies_to_backfill = companies_to_fix_from_bq_df.join(
        tcc_companies_df.select("name"),  # Only need 'name' for the anti-join
        on="name",
        how="left_anti"
    )

    # Step 4: Join with Odoo locations to backfill the master_id.
    # We drop the existing null 'master_id' column before the join.
    backfilled_df = companies_to_backfill.drop("master_id").join(
        dedup_odoo_locations_df,
        on="name",
        how="left"
    ).withColumnRenamed("new_master_id", "master_id") \
     .dropna(subset=['master_id'])  # Keep only records that were successfully matched and filled.

    # Apply the same filtering as other loaders
    combined_ids = CANCELLATION_IDS + GREYSTAR_IDS + AUTO_LOCAL_IDS
    backfilled_df = backfilled_df.filter(~col("master_id").isin(combined_ids))

    # Step 5: Ensure the schema of the backfilled data matches the original tcc_companies_df.
    final_backfilled_df = backfilled_df.select(companies_to_fix_from_bq_df.columns)

    return final_backfilled_df



