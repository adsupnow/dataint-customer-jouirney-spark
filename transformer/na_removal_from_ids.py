from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split


def transform(df):
    
    clean_df = df.dropna(subset=['user_pseudo_id'])
    clean_df = clean_df.withColumn('user_pseudo_id', split(col('user_pseudo_id'), '\\.')[0])

    return clean_df
