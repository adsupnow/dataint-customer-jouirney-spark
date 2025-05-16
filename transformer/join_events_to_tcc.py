from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *

from utils.constants import AUTO_LOCAL_IDS, CANCELLATION_IDS, GREYSTAR_IDS


def transform(events, tcc_session, tcc_companies, tcc_journey):
    # Process tcc_session
    tcc_session = tcc_session \
        .select(*[col(c).alias('tcc_' + c) for c in tcc_session.columns]) \
        .withColumnRenamed('tcc_id', 'tcc_session_id')

    # Process tcc_companies
    tcc_companies = tcc_companies \
        .select(*[col(c).alias('tcc_' + c) for c in tcc_companies.columns]) \
        .withColumnRenamed('tcc_id', 'tcc_company_id') \
        .withColumnRenamed('tcc_name', 'location_name') \
        .withColumnRenamed('tcc_master_id', 'location')

    tcc_journey = tcc_journey.drop("touches")
    tcc_journey_columns = tcc_journey.columns
    for column in tcc_journey_columns:
        tcc_journey = tcc_journey.withColumnRenamed(column, "tcc_" + column)
    tcc_journey = tcc_journey.dropDuplicates(["tcc_auto_lead_id"]).orderBy("tcc_auto_lead_id")
    tcc_journey = tcc_journey.drop("tcc_created_at")
    location_filled = tcc_session.join(tcc_companies, on='tcc_company_id', how='left')

    # Join events with location_filled on 'tcc_session_guid'
    full = events.join(location_filled, on='tcc_session_guid', how='left')

    # Join with tcc_journey on 'tcc_auto_lead_id' and 'tcc_company_id'
    full = full.join(tcc_journey, on=['tcc_auto_lead_id', 'tcc_company_id'], how='left')

    full:DataFrame = full.drop('tcc_created_at_y').withColumnRenamed('tcc_created_at_x', 'tcc_created_at_a')

    combined_ids = CANCELLATION_IDS + GREYSTAR_IDS + AUTO_LOCAL_IDS

    full = full.withColumn("location", trim(col("location").cast("string")))

    full = full.filter((col('location').isNull()) | (~col('location').isin(*combined_ids)) | (col('location')==""))

    return full
