from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField
import re
from typing import Optional
import datetime


def extract_utm_parameter(event_url, parameter):

    if not isinstance(event_url, str):
        return None

    match = re.search(rf'{parameter}=([^&]+)', event_url)
    if match:
        return str(match.group(1)).lower()
    else:
        return None


def determine_allocation(event_url):
    if not isinstance(event_url, str):
        return None, None, None
    utm_medium = extract_utm_parameter(event_url, 'utm_medium')
    utm_content = extract_utm_parameter(event_url, 'utm_content')
    utm_campaign = extract_utm_parameter(event_url, 'utm_campaign')
    utm_term = extract_utm_parameter(event_url, 'utm_term')
    utm_source = extract_utm_parameter(event_url, 'utm_source')

    product = None
    campaign = None
    target = None

    
    if utm_source=='imps.conversionlogix.com' and utm_medium=='search' and utm_content=='demand-gen' and utm_campaign=='demand-gen' and utm_term=='nbc':
        product = 'demand_gen_plus'
    elif utm_source == 'imps.conversionlogix.com' and utm_medium is not None and utm_content is not None and 'search' in utm_medium and 'sitelink' in utm_content:
        product = 'paidsearch_other'
    elif (utm_medium is not None and re.search(r'p?max|performance[- ]max', str(utm_medium), re.IGNORECASE)) or \
         (utm_campaign is not None and re.search(r'p?max|performance[- ]max', str(utm_campaign), re.IGNORECASE)):
        product = 'pmax'
    elif "search" in str(utm_medium):
        product, campaign, target = paidsearch_allocation(utm_medium, utm_content, utm_campaign)       
    elif utm_campaign == "tcc":
        modal_widget = extract_utm_parameter(event_url, "modalwidget")
        if utm_source=='imps.conversionlogix.com' and utm_medium=='email':
            product = "tcc_email_confirmation"
        # elif utm_source=='imps.conversionlogix.com' and ('gmb' in str(utm_medium)):
        #     product = gbpa_allocation(utm_medium, utm_campaign, modal_widget)
        # # TODO: Removed since will end in TCC categories that we dont want
        # else:
        #     product = tcc_allocation(modal_widget, utm_medium, utm_content, utm_campaign)
    elif utm_medium == 'email' and utm_source == 'imps.conversionlogix.com' and utm_campaign is not None and utm_campaign.lower() in ('lead_nurture_email_cm', 'lead_nurture_email_sg'):
            product = "tcc_lead_nurturing"
    elif utm_medium == "display" and (utm_content == "clx" or utm_content == "gt"):
        product, campaign, target = display_allocation(utm_content, utm_campaign)
    elif utm_medium == "display" and utm_content != "clx":
        product, campaign, target = social_allocation(utm_content, utm_campaign)
    elif utm_medium == "video" or utm_medium == "youtube":
        product, campaign, target = youtube_allocation(utm_content, utm_campaign)
    elif utm_medium == "email" and utm_source == "imps.conversionlogix.com" and re.match(r'^\d{4}-[a-zA-Z0-9\-]+$', str(utm_campaign)):
        product = "email"
    elif utm_medium == "email" and utm_source != "imps.conversionlogix.com":
        product = "other_email"
    elif utm_source == 'imps.conversionlogix.com' and utm_medium is not None and 'gmb' in str(utm_medium):
        modal_widget = extract_utm_parameter(event_url, "modalwidget")
        product = gbpa_allocation(utm_medium, utm_campaign, modal_widget)
    elif utm_source == 'imps.conversionlogix.com' and utm_medium is not None and 'gbp' in str(utm_medium):
        modal_widget = extract_utm_parameter(event_url, "modalwidget")
        product = gbpa_allocation(utm_medium, utm_campaign, modal_widget)
    elif utm_source == 'imps.conversionlogix.com' and utm_medium is not None and 'gbpa' in str(utm_medium):
        modal_widget = extract_utm_parameter(event_url, "modalwidget")
        product = gbpa_allocation(utm_medium, utm_campaign, modal_widget)
    else:
        product = None

    return [product, campaign, target]


def determine_allocation_udf(event_url):
    return determine_allocation(event_url)


determine_allocation_spark_udf = udf(determine_allocation_udf, ArrayType(StringType()))


def paidsearch_allocation(utm_medium, utm_content, utm_campaign):
    product = "paidsearch_"
    campaign = None
    target = None

    split_campaign = str(utm_campaign).split('-') #dma-local-branded
    split_content = str(utm_content).split('-')
    if len(split_campaign) > 2:
        campaign = split_campaign[0]
        target = split_campaign[1]
    elif len(split_content) > 2:
        campaign = split_content[0]
        target = split_content[1]

    if utm_medium == 'search' and utm_content == 'sitelink':
        product += 'sitelink'
    elif utm_medium == 'search' and "-" in str(utm_content) and utm_content != 'price-extension':
        product += 'general'
    elif utm_medium == 'paidsearch' and utm_content == 'sitelink':
        product += 'sitelink_ave5'
    elif utm_medium == 'paidsearch' and "-" in str(utm_content) and utm_content != 'price-extension':
        product += 'general_responsive_ad'
    elif utm_medium == 'search' and utm_content == 'price-extension':
        product += 'price_extension'
    else:
        product += "other"

    return product, campaign, target


def tcc_allocation(modal_widget, utm_medium, utm_content, utm_campaign):
    product = "tcc_"

    if utm_medium == "gmb-sg" and utm_content == "google_appt":
        return product + "sg_appt_link"
    elif utm_medium == "gmb-sg" and utm_content == "google_post":
        return product + "sg_post_link"
    elif utm_medium == "gmb-cm":
        return product + "cm_post_link"
    elif utm_medium == "search" and utm_content == "sitelink-sg":
        return product + "sg_sitelink_direct_click"
    elif utm_medium == "search" and utm_content == "sitelink-cm":
        return product + "cm_sitelink_direct_click"
    elif modal_widget == "sg" and utm_content is None:
        return product + "sg_interstitial_general"
    elif modal_widget == "cm" and utm_content is None:
        return product + "cm_interstitial_general"
    else:
        return product + "other"


def display_allocation(utm_content, utm_campaign):
    product = "display_"
    campaign = None
    target = None

    if (utm_content == 'clx'):
        if (utm_campaign == 'targeted_display'):
            product += '3D'
            campaign = utm_campaign
            target = "local"
        elif (utm_campaign == 'retargeting'):
            product += 'rt'
            campaign = utm_campaign
            target = "national"
        elif (utm_campaign == 'cltv'):
            product += 'cltv'
            campaign = utm_campaign
            target = "local"
        elif utm_campaign is not None and 'retargeting_' in utm_campaign:
            product += 'retargeting_premium'
            campaign = utm_campaign
            target = "national"
        elif utm_campaign is not None and 'targeted_display_' in utm_campaign:
            product += 'targeted_display_premium'
            campaign = utm_campaign
            target = "local"
        else:
            product += "other"    
    elif utm_content == 'gt' and 'pmt' in str(utm_campaign):
        product += 'pmt'
        campaign = utm_campaign
    else:
        product += "other"

    return product, campaign, target


def social_allocation(utm_content, utm_campaign):
    product = "social_"
    campaign = None
    target = None
    if utm_campaign == "facebook":
        if utm_content == "fb_retargeting":
            product += 'retargeting'
            campaign = utm_campaign
            target = "national"
        else:
            product += 'dwellers'
            campaign = utm_campaign
            target = "local"
    elif utm_campaign == "instagram":
        if utm_content == "fb_instagram_retargeting":
            product += 'retargeting'
            campaign = utm_campaign
            target = "national"
        elif utm_content == "fb_instagram_stories": #dwellers
            product += 'ig_stories'
            campaign = utm_campaign
            target = "local"
        else:
            product += 'dwellers'
            campaign = utm_campaign
            target = "local"
    elif utm_campaign == "tiktok":
        product += 'tiktok'
        campaign = utm_campaign
        target = "targeted_locations"
    else:
        product += "other"

    return product, campaign, target


def youtube_allocation(utm_content, utm_campaign):
    product = "youtube_"
    campaign = "youtube"
    target = None

    if utm_content == "yt":
        product += "general"
        target = "local"
    elif utm_content == "yt-rt":
        product += "retargeting"
        target = "national"
    elif utm_content == "yt-tva":
        product += "tva"
        target = "local"
    elif utm_content == "yt-tva-rt":
        product += "tva_rt"
        target = "national"
    else:
        product += "other"

    return product, campaign, target


def gbpa_allocation(utm_medium, utm_campaign, modal_widget):
    product = "gbpa_"

    if utm_medium == "gbp-standard":
        if utm_medium == "gbp" and modal_widget == "cm":
            return product + "utm_concession_manager_standard"
        elif utm_medium == "gbp" and modal_widget == "sg":
            return product + "utm_schedule_genie_standard"
        else:
            return product + "utm_base_standard"
    elif utm_medium == "gbp-offer":
        if utm_medium == "gbp" and modal_widget == "cm":
            return product + "utm_concession_manager_offer"
        elif utm_medium == "gbp" and modal_widget == "sg":
            return product + "utm_schedule_genie_offer"
        else:
            return product + "utm_base_offer"
    else:
        return product + "other"

def blank_as_null(x):
    return F.when(F.col(x) != "", F.col(x)).otherwise(None)

# Spark Transformation Function
def transform(df):
    print("⚡ Add CLX Product Categories -", datetime.datetime.now())

    if "product" not in df.columns:
        df = df.withColumn("product", F.lit(None).cast(StringType()))
    if "campaign" not in df.columns:
        df = df.withColumn("campaign", F.lit(None).cast(StringType()))
    if "target" not in df.columns:
        df = df.withColumn("target", F.lit(None).cast(StringType()))

    df = df.withColumn("product", F.when(F.col('event_landing_page_path').isNotNull(), determine_allocation_spark_udf(F.col('event_landing_page_path'))[0]).otherwise(F.col('product')))
    df = df.withColumn("campaign", F.when(F.col('event_landing_page_path').isNotNull(), determine_allocation_spark_udf(F.col('event_landing_page_path'))[1]).otherwise(F.col('campaign')))
    df = df.withColumn("target", F.when(F.col('event_landing_page_path').isNotNull(), determine_allocation_spark_udf(F.col('event_landing_page_path'))[2]).otherwise(F.col('target')))

    # Update "product", "campaign" and "target" based on other columns
    df = df.withColumn("product",
                       F.when((F.col('product').isNull()) & (F.col('event_referrer').isNotNull()),
                              determine_allocation_spark_udf(F.col('event_referrer'))[0])  # Get first value as `product`
                       .otherwise(F.col('product')))

    df = df.withColumn("campaign",
                       F.when((F.col('product').isNull()) & (F.col('event_referrer').isNotNull()),
                              determine_allocation_spark_udf(F.col('event_referrer'))[1])  # Get second value as `campaign`
                       .otherwise(F.col('campaign')))

    df = df.withColumn("target",
                       F.when((F.col('product').isNull()) & (F.col('event_referrer').isNotNull()),
                              determine_allocation_spark_udf(F.col('event_referrer'))[2])  # Get third value as `target`
                       .otherwise(F.col('target')))
    
    df = df.drop("tcc_created_at")
    return df
