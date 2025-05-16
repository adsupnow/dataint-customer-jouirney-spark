from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, isnull
from pyspark.sql.types import StringType, BooleanType
import tldextract
import datetime


# Define a function to extract the domain from a URL
@udf(returnType=StringType())
def extract_domain(url):
    if url is None:
        return None
    return tldextract.extract(url).registered_domain


# Function to check if referrer domain is in landing page path
@udf(returnType=BooleanType())
def referrer_not_in_landing_page(event_referrer_domain, event_landing_page_path):
    if event_referrer_domain is None or event_landing_page_path is None:
        return False
    return event_referrer_domain not in event_landing_page_path


def transform(data, column_to_attribute):
    print("⚡ Address Missing Referrals (Spark) -", datetime.datetime.now())

    # Apply the domain extraction function to the event_referrer column
    spark_df = data.withColumn('event_referrer_domain', extract_domain(col('event_referrer')))

    # Apply the case_when logic using Spark
    spark_df = spark_df.withColumn(
        column_to_attribute,
        when(
            (col(column_to_attribute) == "Direct") &
            (col('event_referrer').isNotNull()) &
            referrer_not_in_landing_page(col('event_referrer_domain'), col('event_landing_page_path')),
            "Referral"
        ).otherwise(col(column_to_attribute))
    )

    # Drop the temporary event_referrer_domain column
    spark_df = spark_df.drop('event_referrer_domain')

    return [spark_df, column_to_attribute]
