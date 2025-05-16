"""A basic Flyte project template that uses ImageSpec"""

import datetime
import random
from operator import add
from flytekit import Resources, task, workflow, ImageSpec
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
from datetime import datetime


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
custom_image = ImageSpec(python_version="3.9", registry="docker.io/clxdataint", packages=["flytekitplugins-spark", "pandas", "numpy", "tldextract", "pyarrow"])


@task(
    task_config=Spark(
        # This configuration is applied to the Spark cluster
        spark_conf={
            "spark.driver.memory": "6000M",
            "spark.executor.memory": "40000M",
            "spark.executor.cores": "4",
            "spark.executor.instances": "4",
            "spark.driver.cores": "1",
            "spark.jars": "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar,https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.32.0/spark-bigquery-with-dependencies_2.12-0.32.0.jar",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "default",
            "spark.kubernetes.authenticate.executor.serviceAccountName": "default",
        }
    ),
    limits=Resources(cpu="17",mem="166000M"),
    container_image=custom_image,
)
def main() -> int:
    spark = flytekit.current_context().spark_session

    start_date = datetime.strptime(os.getenv("STARTDATE"), "%Y-%m-%d")
    end_date = datetime.strptime(os.getenv("ENDDATE"), "%Y-%m-%d")

    event = load_events(start_date=start_date, end_date=end_date, spark=spark)

    odoo_location = load_odoo_location(spark)

    tcc_companies = load_tcc_companies(spark)
    
    tcc_lead_journeys = load_tcc_lead_journeys(start_date, end_date, spark)

    tcc_session = load_tcc_sessions(start_date, end_date, spark)

    df = join_events_to_tcc(event, tcc_session, tcc_companies, tcc_lead_journeys).cache()

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
        .option('table', f'dataint-442318.{os.getenv("stage", "dev")}.combined_table{os.getenv("ENDDATE").replace("-", "")}') \
        .option('temporaryGcsBucket', 'clx-dataint-data') \
        .option('clustering.fields', 'location,user_pseudo_id') \
        .mode('overwrite') \
        .save()
    
    spark.stop()

    return 0

@workflow
def customer_journey_workflow() -> int:
    return main()

if __name__ == "__main__":
    customer_journey_workflow()
    # pyflyte run --remote --service-account default --env STARTDATE=2025-03-01 --env ENDDATE=2025-05-02 customer_journey_workflow.py main 