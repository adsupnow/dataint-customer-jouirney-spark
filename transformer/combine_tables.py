from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, udf, expr, first, split, concat_ws, row_number
from pyspark.sql.window import Window
import datetime
from pyspark.sql.types import StringType


def update_clx_session_id(session_id):
    parts = session_id.split('_')
    if len(parts) > 1:
        parts[1] = '0'
    return '_'.join(parts)


update_clx_session_id_udf = udf(update_clx_session_id, StringType())


def process_dataframe_with_start(df):

    df = df.repartition(col("user_pseudo_id"))
    
    # Define window spec for each user_pseudo_id ordered by event_ts
    window_spec = Window.partitionBy("user_pseudo_id").orderBy("event_ts")
    
    # Add row number within each partition
    df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
    
    # Select only the first row (earliest event_ts) for each user_pseudo_id
    first_rows_df = df_with_row_num.filter(col("row_num") == 1).drop("row_num")

    first_rows_df = first_rows_df.withColumn("user_pseudo_id", col("user_pseudo_id"))

    first_rows_df = first_rows_df.withColumn("event_ts", expr("event_ts - interval 10 seconds"))
    first_rows_df = first_rows_df.withColumn("clx_session_number", lit(0))

    first_rows_df = first_rows_df.withColumn("clx_session_id", concat_ws("_", 
                                                                         split(col("clx_session_id"), "_")[0], 
                                                                         lit("0"),
                                                                         split(col("clx_session_id"), "_")[2],
                                                                         split(col("clx_session_id"), "_")[3]))

    first_rows_df = first_rows_df.withColumn("campaign", lit("((Start))"))
    first_rows_df = first_rows_df.withColumn("channel", lit("((Start))"))

    processed_df = df.unionByName(first_rows_df.select(df.columns), allowMissingColumns=True).orderBy("user_pseudo_id", "event_ts")

    return processed_df


def transform(data1: DataFrame, data2: DataFrame):
    print("⚡ Combining and merge -", datetime.datetime.now())
    df1, column_to_attribute1 = data1
    df2, column_to_attribute2 = data2

    print(column_to_attribute1, column_to_attribute2)

    df1 = df1.withColumn("channel_depth", lit(column_to_attribute1))
    df2 = df2.withColumn("channel_depth", lit(column_to_attribute2))

    df1: DataFrame = process_dataframe_with_start(df1)
    df2: DataFrame = process_dataframe_with_start(df2)

    combined_df = df1.unionByName(df2, True).orderBy("user_pseudo_id")
    return combined_df
