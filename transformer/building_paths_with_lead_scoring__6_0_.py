from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime
from pyspark.sql.types import IntegerType


def transform(data, column_to_attribute):
    print("⚡ Building Paths With Lead Scoring -", datetime.datetime.now())

    # Fill missing values in the specified column
    df = data.withColumn(column_to_attribute, F.coalesce(F.col(column_to_attribute), F.lit('Unknown')))

    # Define event scores
    event_scores = {
        'apply_now': 60, 'contact_form': 60, 'website_contact_form': 60, 'widget_contact_form': 60,
        'concession_claimed': 80, 'tcc___create_lead': 80,
        'tour': 90, 'website_schedule_a_tour': 90, 'widget_schedule_a_tour': 90,
        'other_event': 0
    }

    # Create a UDF to map event names to scores
    event_score_udf = F.udf(lambda x: event_scores.get(x, 0), IntegerType())

    # Map event names to scores and compute session scores
    df = df.withColumn('event_score', event_score_udf(F.col('event_name')))

    # Calculate session_score by getting the maximum event score for each session
    window_spec = Window.partitionBy('user_pseudo_id', 'clx_session_id')
    df = df.withColumn('session_score', F.max('event_score').over(window_spec))

    # Rolling up session paths for each user
    user_session_paths = df.groupBy('user_pseudo_id') \
        .agg(F.concat_ws(' > ', F.collect_set(column_to_attribute)).alias('session_path'))

    # Check if the user has conversion events
    conversion_events = ["virtual_tour","tcc___first_interaction","tcc___create_lead", "get_directions","floorplans", "contact_form","concession_claimed","chat_initiated","calls_from_website", "apply_now","application_submit","email","tour","download_brochure"]
    df_has_conversion = df.groupBy('user_pseudo_id') \
        .agg(F.max(F.when(F.col('event_name').isin(conversion_events), 1).otherwise(0)).alias('has_conversion'))

    # Calculate the user's lead score by summing the max session scores
    user_lead_score = df.groupBy('user_pseudo_id', 'clx_session_id') \
        .agg(F.max('session_score').alias('session_score')) \
        .groupBy('user_pseudo_id') \
        .agg(F.sum('session_score').alias('lead_score'))

    # Merge the dataframes for user session paths, conversion information, and lead score
    user_summary = user_session_paths.join(df_has_conversion, on='user_pseudo_id', how='left') \
        .join(user_lead_score, on='user_pseudo_id', how='left')

    user_summary = user_summary.repartition("session_path")  # Repartition to a single partition for easier handling

    # Roll up the session data
    session_summary = user_summary.groupBy('session_path') \
        .agg(
        F.sum('has_conversion').alias('conversion'),
        (F.count('has_conversion') - F.sum('has_conversion')).alias('no_conversion'),
        F.sum('lead_score').alias('leadscore')
    ) \
        .withColumn('ratio', (F.col('conversion') / (F.col('conversion') + F.col('no_conversion')) * 100).cast('decimal(10,2)')) \
        .orderBy(F.col('conversion'), ascending=False)

    print("⚡ -- u sum")

    return session_summary, column_to_attribute
