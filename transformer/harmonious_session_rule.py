from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime


def transform(data, column_to_attribute):
    print("⚡ Harmonious Session -", datetime.datetime.now())
    data.repartition('user_pseudo_id')

    # Step 1: Sort the data
    window_spec = Window.partitionBy('user_pseudo_id', 'clx_session_id').orderBy('events_ts')

    # Step 2: Get the first non-'Direct' value in each session
    first_non_direct_col = F.when(F.col(column_to_attribute) != 'Direct', F.col(column_to_attribute))

    # Step 3: use first value if all are 'Direct'
    first_non_direct_grouping = F.coalesce(
        F.first(first_non_direct_col, ignorenulls=True).over(window_spec),
        F.first(F.col(column_to_attribute)).over(window_spec)
    )

    # Apply transformation
    df_transformed = data.withColumn(column_to_attribute, first_non_direct_grouping)

    return df_transformed, column_to_attribute
