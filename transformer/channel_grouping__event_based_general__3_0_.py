from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
import datetime

paid_search_medium = ["ppc", "cpc", "paidsearch", "search", "sem", "meta"]
#paid_search_vendors = ["imps.conversionlogix.com", "google", "fb", "adwords", "localiq", "bing", "zillow", "effortless ads", "google-ms"]
paid_search_vendors = ["google", "fb", "adwords", "localiq", "bing", "zillow", "effortless ads", "google-ms"]

# TODO: VERIFY IF THIS IS CORRECT
# Google Pmax
pmax_medium = ["pmax"]
#pmax_source = ["google"]

# Display
display_medium = ["display", "paiddisplay", "banner", "eastads.simpli.fi"]
display_vendors = ["instagram.com", "imps.conversionlogix.com", "zillow", "stackadapt", "localiq", "dhmedia", "ksl", "facebook", "m.facebook.com", "mcc", "altaontherow.com", "display", "apartmentgeofencing.com", "eastads.simpli.fi"]

# Paid Video
paid_video_medium = ["video", "youtube"]
#paid_video_vendor = ["imps.conversionlogix.com", "conversionlogix", "youtube.com", "m.youtube.com", None]
paid_video_vendor = ["imps.conversionlogix.com", "conversionlogix", None]

# TODO: VERIFY IF THIS IS CORRECT
organic_video_medium = ["video"]
organic_video_vendor = ["google"]

# EMAIL
email_medium = ["email", "targeted_email"]

# TODO: VERIFY IF THIS IS CORRECT
# Organic Search  (VERIFY IF THIS IS CORRECT)
organic_medium = ["organic", "googlemybusiness", "google local listing", "bing"]

# TODO: VERIFY IF THIS IS CORRECT
# Organic Social (VERIFY IF THIS IS CORRECT)
organic_social_medium = ["organic_social_post", "social", "socialpost", "organic_social"]
organic_social_vendor = ["instagram", "facebook", "google", "mcw", "apartmentseo"]

# Paid Social
paid_social_medium = ["paidsocial", "paid_social", "social-paid", "ads", "paid", "social_media_sma", "facebook", "paid-social", "cpc-scd", "cpc_scd", "precisionboost", "meta-ad", "ustraffic", "igtraffice"]
paid_social_vendor = ["zillowboost", "facebook", "fb", "ig", "leaselabs", "social kapture", "tiktok", "tiktok.com", "zillow","m.facebook.com", "an", None]

# ILS
ils_medium = ["referral", "ils", "zillow","aptsdotcom", None]
ils_vendor = ["apartments.com", "costar", "aptlist", "ils", "rentpath", "lincolnapts", "homes.com", "apartment list", "zillow", "lm.facebook.com", "apartmentguide.com", "apartnments.com", "rentpath - ils", "listing"]

# SMS
sms_medium = ["sms"]
sms_vendor = ["rentpath - ils", "google.com", "apartments.com", "apartment list", "google.com,google.com", "imps.conversionlogix.com", "zillow,zillow", "rentpath - social"]

# Business Listing
business_listing_medium = ["referral", "yext", "gmb", "yelp site", "listing", "ils", "googlebusinessprofile", "gbp", "gmb", "gbp-sg", "google", "googlepost", "button"]
business_listing_vendor = ["apartmentseo", "google_business", "yelp", "gmb", "yext", "gbp", "extnet", "yelp.com", "fr.yelp.ca", "yelp.com", "m.yelp.com", "yelp-sales--c.vf.force.com", "ils", "apartmentseo", "google", "imps.conversionlogix.com", None]


clean_columns = [
            "user_pseudo_id",
            "product",
            "campaign",
            "target",
            "event_referrer",
            "event_landing_page_path",
            "event_name",
            "event_source",
            "traffic_source_source",
            "tcc_utm_source",
            "event_medium",
            "traffic_source_medium",
            "tcc_utm_medium",
            "event_ts",
            "tcc_created_at",
            "event_campaign",
            "tcc_utm_campaign",
            "event_session_id",
            "event_term",
            "tcc_utm_term",
            "event_content",
            "tcc_utm_content",
            "event_session_number",
            "traffic_source_name",
            "tcc_session_id",
            "tcc_machine_guid",
            "tcc_region",
            "tcc_city",
            "tcc_ip",
            "tcc_first_name",
            "tcc_last_name",
            "tcc_email",
            "tcc_phone_number",
            "tcc_lead_type",
            "tcc_company_id",
            "tcc_company_name",
            "tcc_odoo_master_id",
            "channel",
            "location",
            "location_name",
        ]

# CLX Channel Grouping UDF
def clx_channel_grouping(product, event_medium, event_source):
    if product is None or pd.isna(product):
        return "Unknown"

    if product == "email":
        return "CLX Email"
    elif "other_email" in product:
        return "Other Email"
    elif "tcc_email_confirmation" in product:
        return "CLX TCC"
    elif "tcc_lead_nurturing" in product:
        return "CLX TCC"
    elif "youtube_" in product:
        return "CLX YouTube"
    elif "display_" in product:
        return "CLX Display"
    elif "paidsearch_other" in product:
        return "CLX Paid Search"
    elif "paidsearch" in product:
        if 'conversionlogix' in event_source:
            return "CLX Paid Search"
    elif "gbpa_" in product:
        return "CLX gbpa"
    elif "social_" in product:
        return "CLX Social"
    elif "demand_gen_" in product:
        return "CLX DGP"
    elif "pmax" in product:
        return "CLX Pmax"
    elif event_medium and event_source:
        if event_medium in paid_search_medium and event_source in paid_search_vendors:
            return "Other Paid Search"
        elif event_medium in paid_video_medium and event_source in paid_video_vendor:
            return "Other YouTube"
        elif event_medium in email_medium:
            return "Other Email"
        elif event_medium in display_medium and event_source in display_vendors:
            return "Other Display"
        elif (
            event_medium in business_listing_medium
            and event_source in business_listing_vendor
        ):
            return "Other gbpa"
        elif event_medium in paid_social_medium and event_source in paid_social_vendor:
            return "Other Paid Social"
        elif (
            event_medium in organic_social_medium
        ):
            return "Organic Social"
        # elif event_medium in pmax_medium:
        #     if event_source in pmax_source:
        #         return "CLX Pmax"
        #     else:
        #         return "Other Pmax"
        elif event_medium in organic_medium:
            return "Organic Search"
        elif event_source == "direct":
            return "Direct"
        elif event_medium == "referral" and event_source not in business_listing_vendor:
            return "Referral"
        elif (
            event_medium in organic_video_medium
        ):
            return "Organic Video"
        elif event_medium in ils_medium and event_source in ils_vendor:
            return "ILS"
        elif event_medium in sms_medium and event_source in sms_vendor:
            return "SMS"
        elif event_medium == "(none)" and event_source == "(direct)":
            return "Direct"
        else:
            return "Unknown"
    else:
        return "Unknown"


# Register UDF
clx_channel_grouping_udf = udf(clx_channel_grouping, StringType())


def construct_campaign_grouping_udf(product, campaign, target):
    if product and campaign and target:
        components = ["CLX"]

        if product:
            components.append(product.replace('_', ' '))
        if campaign:
            components.append(campaign.replace('_', ' '))
        if target:
            components.append(target.replace('_', ' '))
        return '_'.join(components)
    else:
        return "Unknown"


# Register UDF
construct_campaign_grouping_udf = udf(construct_campaign_grouping_udf, StringType())


def construct_clx_campaign_attribution_udf(product, campaign, target):
    if pd.isna(product) or product is None:
        return "Unknown"
    
    if "paidsearch_other" in product:
        return "CLX_paidsearch_sitelink"
    elif "pmax" in product:
        return "CLX_pmax"
    elif "paidsearch" in product:
        if campaign == "brand":
            if target == "local":
                return "CLX_paidsearch_brand_local"
            else:
                return "CLX_paidsearch_brand_nonlocal"
        else:
            if target == "local":
                return "CLX_paidsearch_nonbrand_local"
            else:
                return "CLX_paidsearch_nonbrand_nonlocal"
    elif "youtube" in product:
        if product == "youtube_general":
            return "CLX_youtube_yt"
        elif product == "youtube_rt":
            return "CLX_youtube_yt-rt"
        elif product == 'youtube_tva':
            return "CLX_youtube_yt-tva"
        elif product == 'youtube_tva_rt':
            return "CLX_youtube_yt-tva-rt"
        else:
            return "Unknown"
    elif "display" in product:
        if campaign is not None and "retargeting" in campaign:
            return "CLX_display_retargeting"
        elif campaign is not None and "targeted" in campaign:
            return "CLX_display_targeted"
        elif campaign is not None and "pmt" in campaign:
            return "CLX_display_pmt"
        elif campaign is not None and "cltv" in campaign:
            return "CLX_display_cltv"
        else:
            return "Unknown"
    elif "social" in product:
        if "retargeting" in product:
            return "CLX_social_retargeting"
        elif "dwellers" in product:
            return "CLX_social_dwellers"
        elif "ig_stories" in product:
            return "CLX_social_ig_stories"
        elif "tiktok" in product:
            return "CLX_social_tiktok"
        return "Unknown"
    elif product == "email":
        return "CLX_email"
    elif "other_email" in product:
        return "Other Email"
    elif "tcc_email_confirmation" in product:
        return "CLX_tcc_email_confirmation"
    elif "tcc_lead_nurturing" in product:
        return "CLX_tcc_lead_nurturing"
    elif "gbpa" in product:
        return "CLX_gbpa"
    elif "demand_gen" in product:
        return "CLX_dgp"
    else:
        return "Unknown"


# Register UDF
construct_clx_campaign_attribution_spark_udf = udf(construct_clx_campaign_attribution_udf, StringType())


def transform(df, column_to_attribute):
    print("⚡ Channel Grouping: Event Based (general) -", datetime.datetime.now())

    # Remove spam
    spammers = [
        "hiwpro.xyz", "news.grets.store", "rida.tokyo", "kar.razas.site", "static.seders.website", "game.fertuk.site",
        "trast.mantero.online", "ofer.bartikus.site", "garold.dertus.site", "hiwpro.xyz", "blogsmith.online", "crm.xiaoman.cn", "jackonline.store"
    ]
    clean = df.filter(~df['event_source'].isin(spammers))

    # Convert columns to lowercase
    clean = clean.select([col(c).cast("string").alias(c) for c in clean.columns if c in clean_columns])
    # Use F.lower() function to convert columns to lowercase
    clean = clean.select([F.when(F.col(c).isNotNull(), F.lower(F.col(c))).otherwise(F.col(c)).alias(c) for c in clean.columns])

    # Add datasource column
    clean = clean.withColumn('datasource', F.lit('ga'))

    # Fill NaN with ''
    clean = clean.fillna({"product": "", "campaign": "", "target": ""})

    # Add clx_channel_grouping column
    clean = clean.withColumn("clx_channel_grouping", clx_channel_grouping_udf(col("product"), col("event_medium"), col("event_source")))

    if column_to_attribute == 'campaign_attribution':
        # Address the attributions based on products
        clean = clean.withColumn("campaign_attribution", construct_clx_campaign_attribution_spark_udf(col('product'), col('campaign'), col('target')))

        # Update campaign_attribution column
        clean = clean.withColumn("campaign_attribution", when(
            (col('campaign_attribution') == "Unknown") & (col('clx_channel_grouping') != "CLX Paid Search"),
            col('clx_channel_grouping')
        ).otherwise(col('campaign_attribution')))

        # Remove clx_channel_grouping column
        clean = clean.drop('clx_channel_grouping')

    return [clean, column_to_attribute]
