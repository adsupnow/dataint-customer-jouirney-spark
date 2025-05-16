from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime
from pyspark.sql.functions import lit


def transform(data, column_to_attribute):
    print("⚡ Last Direct -", datetime.datetime.now())

    data.repartition('user_pseudo_id')
    # Define a window spec to partition by user_pseudo_id and order by events_ts
    window_spec = Window.partitionBy('user_pseudo_id').orderBy('events_ts')

    # Create a column that stores the last non-direct value for each user_pseudo_id
    df = data.withColumn('last_non_direct',
                         F.when(data[column_to_attribute] != 'Direct', data[column_to_attribute]).otherwise(F.lit(None)))

    # Fill the 'last_non_direct' column with forward fill and backward fill
    df = df.withColumn('last_non_direct', F.last('last_non_direct', ignorenulls=True).over(window_spec))

    # Update the original column with the last non-direct value for 'Direct' sessions
    df = df.withColumn(column_to_attribute,
                       F.when((df[column_to_attribute] == 'Direct') & (df['last_non_direct'].isNotNull()), df['last_non_direct'])
                       .otherwise(df[column_to_attribute]))

    # Drop the temporary 'last_non_direct' column
    df = df.drop('last_non_direct')

    return df, column_to_attribute