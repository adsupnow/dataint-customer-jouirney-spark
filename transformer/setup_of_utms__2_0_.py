from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def transform(df):
    # Fill missing 'event_source' values with 'traffic_source_source'
    df = df.withColumn(
        "event_source",
        when(col("event_source").isNotNull(), col("event_source"))
            .when(col("traffic_source_source").isNotNull(), col("traffic_source_source"))
            .otherwise("NA")
    )

    # Fill missing 'event_medium' values with 'traffic_source_medium'
    df = df.withColumn(
        "event_medium",
        when(col("event_medium").isNotNull(), col("event_medium"))
            .when(col("traffic_source_medium").isNotNull(), col("traffic_source_medium"))
            .otherwise("NA")
    )

    # Martin's approach: Overwrite GA Events with TCC values
    df = df.withColumn(
        "event_source",
        when(col("event_source").isNull(), col("tcc_utm_source"))
            .otherwise(col("event_source"))
    )

    df = df.withColumn(
        "event_medium",
        when(col("event_medium").isNull(), col("tcc_utm_medium"))
            .otherwise(col("event_medium"))
    )

    return df
