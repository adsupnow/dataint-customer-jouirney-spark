import datetime
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, pandas_udf, lit, when, broadcast
from pyspark.sql.types import StructType, StructField, StringType
import traceback

from utils.url_utils import normalize_url
from utils.trie_utils import Trie, find_most_specific_match_trie

def transform(joined_data: DataFrame, odoo_locations: DataFrame, spark: SparkSession, ) -> DataFrame:
    """
    PySpark implementation of apply_location that uses a Trie for URL matching.
    
    Args:
        spark: SparkSession
        joined_data: PySpark DataFrame with event data
        odoo_locations: PySpark DataFrame with location data
        
    Returns:
        DataFrame: PySpark DataFrame with updated location information
    """
    print(">> Fix missing Location (PySpark) - %s", datetime.datetime.now())
    
    try:
        # Check if required columns exist
        if 'event_landing_page_path' not in joined_data.columns:
            print("'event_landing_page_path' column not found. Using empty strings.")
            joined_data = joined_data.withColumn('event_landing_page_path', lit(""))
        
        if 'website' not in odoo_locations.columns:
            print("'website' column not found in odoo_locations. Cannot proceed.")
            return joined_data
        
        # Step 1: Normalize URLs in the joined data
        print(">> Normalizing URLs in joined data - %s", datetime.datetime.now())
        normalize_url_udf = udf(normalize_url, StringType())
        df = joined_data.withColumn('url', normalize_url_udf(col('event_landing_page_path')))
        
        # Step 2: Process locations data on driver node
        print(">> Processing locations data - %s", datetime.datetime.now())
        locations_pd = odoo_locations.toPandas()
        
        # Normalize URLs in locations DataFrame
        locations_pd['url'] = locations_pd['website'].apply(normalize_url)
        
        # Clean and prepare locations DataFrame
        locations_pd = (
            locations_pd.dropna()
            .rename(columns={'name': 'location_name', 'master_id': 'location'})
            .astype({'location': str})
            .drop_duplicates()
        )
        
        # Remove invalid URLs
        locations_pd = locations_pd[locations_pd['url'].str.strip().str.len() > 0]
        locations_pd = locations_pd[locations_pd['url'] != '-']
        
        # Sort by URL length (descending)
        locations_pd['url_length'] = locations_pd['url'].apply(len)
        locations_sorted = locations_pd.sort_values(by='url_length', ascending=False).drop(columns='url_length')
        
        # Step 3: Build Trie on driver node
        print(">> Building Trie - %s", datetime.datetime.now())
        trie = Trie()
        for index, row in locations_sorted.iterrows():
            trie.insert(row['url'], row['location'], row['location_name'])
        
        # Step 4: Broadcast the Trie to all executors
        broadcast_trie = spark.sparkContext.broadcast(trie)
        
        # Step 5: Define a UDF that uses the Trie
        schema = StructType([
            StructField("location", StringType(), True),
            StructField("location_name", StringType(), True)
        ])
        
        @pandas_udf(schema)
        def find_location_udf(urls: pd.Series) -> pd.DataFrame:
            results = []
            t = broadcast_trie.value
            for url in urls:
                location, location_name = find_most_specific_match_trie(url, t)
                results.append((location, location_name))
            return pd.DataFrame(results, columns=["location", "location_name"])
        
        # Step 6: Apply the UDF to find matches for missing locations
        print(">> Finding matching locations - %s", datetime.datetime.now())
        missing_df = df.filter(col('location').isNull())

        existing_location_df = df.filter(col('location').isNotNull())
        
            # Apply the UDF to find matching locations
        matched_df = missing_df.repartition(1500).select(
            "*", 
            find_location_udf(col('url')).alias('location_match')
        )
        
        after_matched_df = matched_df \
            .withColumn("location", col("location_match.location")) \
            .withColumn("location_name", col("location_match.location_name")) \
            .drop("location_match")
        
        df = existing_location_df.unionAll(after_matched_df)

        df = df.drop("url")
        df = df.withColumn("tcc_odoo_master_id", when(col("tcc_odoo_master_id").isNull(), col("location")).otherwise(col("tcc_odoo_master_id")))        
        
        return df
    
    except Exception as e:
        print(">> Unexpected error in apply_location_spark: %s", str(e))
        print(">> Full stack trace:\n%s", traceback.format_exc())
        return spark.createDataFrame([], schema=joined_data.schema)