"""A basic Flyte project template that uses ImageSpec"""

import datetime
import random
from operator import add
from typing import Optional, Any
from flytekit import Resources, task, workflow, dynamic
from flytekit.image_spec import ImageSpec
from flytekitplugins.spark import Spark
import flytekit
from loaders.data_loader import *
from pyspark.sql.functions import col
from transformer.join_events_to_tcc import transform as join_events_to_tcc
from transformer.na_removal_from_ids import transform as na_removal_from_ids
from transformer.setup_of_utms__2_0_ import transform as setup_of_utms
from transformer.add_clx_product_categories import transform as add_clx_product_categories
from transformer.channel_grouping__event_based_general__3_0_ import transform as channel_grouping_event
from transformer.address_missing_referrals import transform as address_missing_referrals
from transformer.splitting_session import transform as splitting_session
from transformer.last_direct import transform as last_direct
from transformer.harmonious_session_rule import transform as harmonious_session_rule
from transformer.building_paths_with_lead_scoring__6_0_ import transform as building_paths_with_lead_scoring
from transformer.clx_session_model__4_0_ import transform as clx_session_model
from transformer.combine_tables import transform as combined_tables
from transformer.apply_location_v2 import transform as apply_location_v2
from transformer.update_company_master_ids import transform as update_company_master_ids
from transformer.concat_dataframes import transform as concat_dataframes
from datetime import datetime, timedelta
from flytekit.core.schedule import CronSchedule
from flytekit import LaunchPlan

"""
ImageSpec is a way to specify a container image configuration without a
Dockerfile. To use ImageSpec:
1. Add ImageSpec to the flytekit import line.
2. Uncomment the ImageSpec definition below and modify as needed.
3. If needed, create additional image definitions.
4. Set the container_image parameter on tasks that need a specific image, e.g.
`@task(container_image=basic_image)`. If no container_image is specified,
flytekit will use the default Docker image at
https://github.com/flyteorg/flytekit/pkgs/container/flytekit.

For more information, see the
`ImageSpec documentation <https://docs.flyte.org/projects/cookbook/en/latest/auto_examples/customizing_dependencies/image_spec.html#image-spec-example>`__.
"""
# Update ImageSpec with specific compatible versions
custom_image = ImageSpec(
    python_version="3.9", 
    registry="docker.io/clxdataint", 
    packages=[
        "flytekit",  # Specify a specific flytekit version
        "flytekitplugins-spark",  # Make sure this matches the flytekit version
        "marshmallow_enum",  # Add the missing dependency
        "numpy",  # Pin numpy to a specific version
        "pandas",  # Pin pandas to a version compatible with numpy 1.23.5
        "tldextract",
        "pyarrow"  # Updated to be compatible with flytekit 1.5.0 (which requires <11.0.0)
    ]
)

dynamic_workflow_image = ImageSpec(
    python_version="3.9", 
    registry="docker.io/clxdataint", 
    packages=[
        "flytekit",  # Specify a specific flytekit version # Make sure this matches the flytekit version
        "marshmallow_enum",  # Add the missing dependency # Pin pandas to a version compatible with numpy 1.23.5
    ]
)



@task(
    task_config=Spark(
        # This configuration is applied to the Spark cluster
        spark_conf={
            "spark.driver.memory": "6000M",
            "spark.executor.memory": "40000M",
            "spark.executor.cores": "4",
            "spark.executor.instances": "20",
            "spark.driver.cores": "1",
            "spark.jars": "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar,https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.32.0/spark-bigquery-with-dependencies_2.12-0.32.0.jar",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "default",
            "spark.kubernetes.authenticate.executor.serviceAccountName": "default",
            "spark.sql.shuffle.partitions": "1500",
        }
    ),
    limits=Resources(cpu="130",mem="1340000M"),
    container_image=custom_image,
)
def main(end_of_end_date: Optional[str] = None, start_of_end_date: Optional[str]=None, look_back: Optional[str] = None) -> int:
    spark = flytekit.current_context().spark_session
    
    today = datetime.now()
    if end_of_end_date is None:
        end_of_end_date = today.strftime("%Y-%m-%d")
    if start_of_end_date is None:
        start_of_end_date = today.strftime("%Y-%m-%d")

    end_date = datetime.strptime(end_of_end_date, "%Y-%m-%d")
    global res
    res = None
    while end_date >= datetime.strptime(start_of_end_date, "%Y-%m-%d"):

        execute(spark, end_date=end_date.strftime("%Y-%m-%d"), look_back=look_back)
        end_date -= timedelta(days=1)

    spark.stop()
    return 0
    # return execute(spark, end_date, look_back)

def execute(spark: Any, end_date: Optional[str] = None, look_back: Optional[str] = None, upstream_result: Optional[str] = None) -> int:

    today =  datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else today
    look_back_original = int(look_back) if look_back else 30
    look_back_original_str = str(look_back_original)
    look_back = look_back_original+1 if end_date == today else look_back_original
    start_date = end_date - timedelta(days=look_back) 
    end_date_str = end_date.strftime("%Y%m%d")
    

    event = load_events(start_date=start_date, end_date=end_date, spark=spark)

    odoo_location = load_odoo_location(spark)

    tcc_companies = load_tcc_companies(spark)

    backfilled_companies = load_backfilled_companies(spark, tcc_companies)

    combined_companies = concat_dataframes(tcc_companies, backfilled_companies)

    updated_tcc_companies = update_company_master_ids(
        tcc_companies=combined_companies,
        odoo_locations=odoo_location
        )
    
    tcc_lead_journeys = load_tcc_lead_journeys(start_date, end_date, spark)

    tcc_session = load_tcc_sessions(start_date, end_date, spark)

    df = join_events_to_tcc(event, tcc_session, updated_tcc_companies, tcc_lead_journeys).cache()

    df = apply_location_v2(df, odoo_location, spark)

    df = na_removal_from_ids(df)

    df = setup_of_utms(df)

    clx_product_categories_df = add_clx_product_categories(df)

    df = channel_grouping_event(clx_product_categories_df, "clx_channel_grouping")

    df = address_missing_referrals(df[0], "clx_channel_grouping")

    df = clx_session_model(df[0], "clx_channel_grouping")

    df = splitting_session(df[0], "clx_channel_grouping")

    df = last_direct(df[0], "clx_channel_grouping")

    hdf = harmonious_session_rule(df[0], "clx_channel_grouping")

    end = building_paths_with_lead_scoring(hdf[0], "clx_channel_grouping")

    # ########################################################################################################################

    df = channel_grouping_event(clx_product_categories_df, "campaign_attribution")

    df = address_missing_referrals(df[0], "campaign_attribution")

    df = clx_session_model(df[0], "campaign_attribution")

    df = splitting_session(df[0], "campaign_attribution")

    df = last_direct(df[0], "campaign_attribution")

    hdf2 = harmonious_session_rule(df[0], "campaign_attribution")

    end2 = building_paths_with_lead_scoring(hdf2[0], "campaign_attribution")

    result = combined_tables(hdf, hdf2)
    
    result.write.format('bigquery') \
        .option('table', f'dataint-442318.{os.getenv("stage", "dev")}.combined_table_{look_back_original_str}_days_look_back{end_date_str}') \
        .option('temporaryGcsBucket', 'clx-dataint-data') \
        .option('clustering.fields', 'location,user_pseudo_id') \
        .mode('overwrite') \
        .save()


    # result.write.format('bigquery') \
    # .option('table', f'dataint-442318.{os.getenv("stage", "dev")}.cj_investigation_table') \
    # .option('temporaryGcsBucket', 'clx-dataint-data') \
    # .option('clustering.fields', 'location,user_pseudo_id') \
    # .mode('overwrite') \
    # .save()
    

    return 0

@workflow()
def customer_journey_workflow(kickoff_time: datetime = None,
                              end_of_end_date: Optional[str] = None,
                              start_of_end_date: Optional[str] = None,
                              look_back: Optional[str] = None) -> int:
    # Add kickoff_time as an input parameter, defaulting to None
    return main(end_of_end_date=end_of_end_date, start_of_end_date=start_of_end_date, look_back=look_back)


daily_customer_journey_workflow = LaunchPlan.get_or_create(
    workflow=customer_journey_workflow,
    name="daily_customer_journey_workflow_30days",  # Fixed the name to match the workflow
    schedule=CronSchedule(
        schedule="0 0 * * *",
        kickoff_time_input_arg="kickoff_time"
    ),
    auto_activate=True
)


    # To run the workflow manually:
    # pyflyte run --remote --service-account default customer_journey_workflow.py customer_journey_workflow --kickoff_time 2023-01-01T00:00:00
    # 
    # Alternatively, if you don't need to specify a kickoff time, you can run the main task directly:
    # pyflyte run --remote --service-account default customer_journey_workflow.py main
    
    # To register the workflow with its schedule:
    # pyflyte register --project flytesnacks --domain development --service-account default customer_journey_workflow.py

