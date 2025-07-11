import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(
    tcc_companies: DataFrame, 
    odoo_locations: DataFrame
) -> DataFrame:
    """
    Update missing master_ids in tcc_companies by matching with odoo_locations (PySpark version).
    
    This function:
    1. Normalizes company names in both dataframes for case-insensitive matching.
    2. Updates master_id in tcc_companies where it's null by matching company names with odoo_locations.
    
    Args:
        tcc_companies (DataFrame): Spark DataFrame containing company information from TCC.
                                   Expected columns: 'name', 'master_id', ...
        odoo_locations (DataFrame): Spark DataFrame containing location information from Odoo.
                                    Expected columns: 'name', 'master_id', ...
        
    Returns:
        DataFrame: Updated tcc_companies Spark DataFrame with filled master_ids where possible.
    """
    print(f">> Updating company master IDs (Spark) - {datetime.datetime.now()}")

    # Define a common expression for name normalization to ensure consistency.
    # This handles nulls, converts to lowercase, and standardizes whitespace.
    normalization_expr = F.trim(F.lower(F.regexp_replace(F.coalesce(F.col("name"), F.lit("")), "\\s+", " ")))

    # Prepare Odoo locations data: normalize name and select relevant columns
    odoo_locs_prepared = odoo_locations.withColumn(
        "normalized_name",
        normalization_expr
    )
    
    # Create a mapping of normalized names to master_ids from Odoo.
    # Drop duplicates to handle cases where a name might have multiple entries.
    name_to_master_map = odoo_locs_prepared.select(
        "normalized_name", 
        F.col("master_id").alias("new_master_id")
    ).dropDuplicates(["normalized_name"])

    # Prepare TCC companies data: normalize name
    tcc_companies_prepared = tcc_companies.withColumn(
        "normalized_name",
        normalization_expr
    )

    # Left join TCC companies with the Odoo mapping on the normalized name
    companies_joined = tcc_companies_prepared.join(
        name_to_master_map,
        "normalized_name",
        "left"
    )

    # Update master_id: if master_id is null, use the one from Odoo, otherwise keep original.
    companies_updated = companies_joined.withColumn(
        "master_id",
        F.when(
            F.col("master_id").isNull(), 
            F.col("new_master_id")
        ).otherwise(F.col("master_id"))
    )

    # Drop the temporary columns used for the join
    final_df = companies_updated.drop("normalized_name", "new_master_id")
    
    return final_df