from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lag, unix_timestamp, lit, concat
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum as spark_sum, min as spark_min
from pyspark.sql.functions import col, to_timestamp, unix_timestamp
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import tldextract
import datetime
from utils.constants import CONVERSION_EVENTS


# Define a function to extract the domain from a URL
def extract_domain(url):
    if url is None:
        return None
    return tldextract.extract(url).registered_domain


extract_domain_udf = udf(extract_domain, StringType())

# Define constants
splitrule = 1  # min for inactivity when channel grouping changes


# Main transformation function
def transform(data, column_to_attribute, split_on_conversion=True):
    print("⚡ Splitting Session -", datetime.datetime.now())

    df = data

    # Apply domain extraction and check for securecafe
    df = df.withColumn('referrer_domain', extract_domain_udf(col('event_referrer')))
    df = df.withColumn('is_securecafe', col('referrer_domain').like("%securecafe.com%"))
    df = df.withColumn('is_conversion_event', col('event_name').isin(*CONVERSION_EVENTS))

    df = df.withColumn('events_ts', to_timestamp(col('events_ts'), 'yyyy-MM-dd HH:mm:ss'))

    window_spec = Window.partitionBy('user_pseudo_id', 'clx_session_number').orderBy('events_ts')

    # Calculate time difference in minutes
    df = df.withColumn('time_diff',
                       (unix_timestamp(col('events_ts')) - unix_timestamp(lag('events_ts', 1).over(window_spec))) / 60)

    df = df.fillna({'time_diff': 0})

    # Detect change in attribution grouping
    change_in_grouping_condition = (
            (col(column_to_attribute) != lag(column_to_attribute, 1).over(window_spec)) &
            (~col('is_securecafe')) &
            (col(column_to_attribute) != "Direct") &
            (col('time_diff') >= splitrule)
    )

    # calc change_in_attribution_grouping
    df = df.withColumn("change_in_attribution_grouping", when(change_in_grouping_condition, 1).otherwise(0))

    # cumsum in spark special window
    window_spec_cumsum = Window.partitionBy('user_pseudo_id', 'clx_session_number').orderBy('events_ts').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn('sub_session_id', spark_sum(col('change_in_attribution_grouping')).over(window_spec_cumsum))

    window_spec_subsession_cumsum = Window.partitionBy('user_pseudo_id', 'clx_session_number', "sub_session_id").orderBy('events_ts').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn('conversion_sum', spark_sum(when(col('event_name').isin(*CONVERSION_EVENTS), 1).otherwise(0)).over(window_spec_subsession_cumsum))

    if split_on_conversion:
    # Create final session ID
        df = df.withColumn('final_session_id',
                        concat(col('user_pseudo_id').cast('string'), lit('_'),
                                col('clx_session_number').cast('string'), lit('_'),
                                col('sub_session_id').cast('string'), lit('_'),
                                col('conversion_sum').cast('string')))
        
        df = df.drop("conversion_sum")

    else:

        df = df.withColumn('final_session_id',
                        concat(col('user_pseudo_id').cast('string'), lit('_'),
                                col('clx_session_number').cast('string'), lit('_'),
                                col('sub_session_id').cast('string')))
        
        df_sub_session_min_ts = df.filter(col('conversion_sum') == 1).groupBy('final_session_id') \
            .agg(spark_min('events_ts').alias('min_conversion_event_ts'))
        
        df = df.join(df_sub_session_min_ts, on='final_session_id', how='left') \
               .filter(col('events_ts') <= col('min_conversion_event_ts'))
        
        df = df.drop("min_conversion_event_ts", 'conversion_sum')
        

    # Avoid duplicating clx_session_id column
    if 'clx_session_id' in df.columns:
        df = df.drop('clx_session_id')

    # Rename final_session_id to clx_session_id
    df = df.withColumnRenamed('final_session_id', 'clx_session_id')

    return df, column_to_attribute
