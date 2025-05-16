from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime


def transform(data, column_to_attribute, inactivity_window_hours=24):
    print("⚡ CLX Session Model -", datetime.datetime.now())

    # make sure inactivity_window_hours is int
    if isinstance(inactivity_window_hours, str):
        inactivity_window_hours = int(inactivity_window_hours)

    df_sessions = data.withColumn('events_ts', F.col('event_ts').cast('timestamp'))

    df_sessions = df_sessions.repartition('user_pseudo_id')

    window_spec = Window.partitionBy('user_pseudo_id').orderBy('events_ts')

    # calc time diff
    df_sessions = df_sessions.withColumn('time_diff',
                                         (F.unix_timestamp('events_ts') - F.unix_timestamp(F.lag('events_ts').over(window_spec))) / 60)

    df_sessions = df_sessions.fillna({'time_diff': 0})

    # if the time difference is greater than inactivity_window_hours, then it is a new session
    inactivity_seconds = inactivity_window_hours * 3600
    df_sessions = df_sessions.withColumn('clx_session_number',
                                         (F.col('time_diff') > inactivity_seconds / 60).cast('int'))

    # 累加计算 session number
    df_sessions = df_sessions.withColumn('clx_session_number',
                                         F.sum('clx_session_number').over(window_spec) + 1)

    # 生成 clx_session_id
    df_sessions = df_sessions.withColumn('clx_session_id',
                                         F.concat(F.col('user_pseudo_id').cast('string'),
                                                  F.lit('_'),
                                                  F.col('clx_session_number').cast('string')))

    return [df_sessions, column_to_attribute]
