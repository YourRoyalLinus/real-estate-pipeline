# Databricks notebook source
# DBTITLE 1,Configure S3 access
access_key = dbutils.secrets.get(scope = "aws", key = "aws-access-key")
secret_key = dbutils.secrets.get(scope = "aws", key = "aws-secret-key")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

aws_region =  dbutils.widgets.get("aws_region")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3." + aws_region + ".amazonaws.com")

# COMMAND ----------

# DBTITLE 1,Load batch into DataFrame
from datetime import date
from pyspark.sql.types import *

bucket_name = dbutils.widgets.get("aws_bucket_name")
endpoint =  dbutils.widgets.get("data_endpoint")
timestamp = date.today()
bucket_key = f"/{timestamp}/{endpoint}_{timestamp}.json.gz"
    
file_type = "json"
file_location = f"s3a://{bucket_name}/{bucket_key}"

properties_df_schema = StructType([
  StructField("properties",
     ArrayType(
         StructType([
            StructField("address", 
                StructType([
                    StructField("city", StringType()),
                    StructField("country", StringType()),
                    StructField("county", StringType()),
                    StructField("county_needed_for_uniq", BooleanType()),
                    StructField("fips_code", StringType()),
                    StructField("lat", DoubleType()),
                    StructField("line", StringType()), 
                    StructField("lon", DoubleType()),
                    StructField("neighborhood_name", StringType()),
                    StructField("neighborhoods", 
                        ArrayType(StructType([
                                StructField("city", StringType()),
                                StructField("id", StringType()), 
                                StructField("level", StringType()),
                                StructField("name", StringType()), 
                                StructField("state_code", StringType()),
                        ]))),
                    StructField("postal_code", StringType()),
                    StructField("state", StringType()), 
                    StructField("state_code", StringType()),
                    StructField("street", StringType()), 
                    StructField("street_number", StringType()), 
                    StructField("street_suffix", StringType()),
                    StructField("time_zone", StringType()),
                    StructField("unit", StringType())
                ])
            ),
            StructField("agents", 
                ArrayType(StructType([
                    StructField("advertiser_id", StringType()), 
                    StructField("id", StringType()), 
                    StructField("name", StringType()),
                    StructField("photo", StructType([StructField("href", StringType())])),
                    StructField("primary", BooleanType())                                                          
                ]))
            ),
            StructField("baths", LongType()), 
            StructField("baths_full", LongType()),
            StructField("baths_half", LongType()),
            StructField("beds", LongType()),
            StructField("branding", 
                StructType([
                    StructField("listing_office", StructType([
                        StructField("list_item", StructType([
                            StructField("accent_color", StringType()),
                            StructField("link", StringType()),
                            StructField("name", StringType()),
                            StructField("phone", StringType()),
                            StructField("photo", StringType()),
                            StructField("show_realtor_logo", BooleanType()),
                            StructField("slogan", StringType()),
                         ]))                    
                    ])),
                    StructField("state_license", StringType()),
                ])
            ),  
            StructField("broker", 
                StructType([
                    StructField("advertiser_id", LongType()), 
                    StructField("name", StringType())
                ])
            ), 
            StructField("building_size", 
                StructType([
                    StructField("size", LongType()), 
                    StructField("units", StringType())
                ])
            ),                  
            StructField("client_display_flags", 
                StructType([
                    StructField("flip_the_market_enabled", BooleanType()), 
                    StructField("has_open_house", BooleanType()), 
                    StructField("has_specials", BooleanType()), 
                    StructField("is_apartmentlist", BooleanType()),
                    StructField("is_co_broke_email", BooleanType()),
                    StructField("is_co_broke_phone", BooleanType()),
                    StructField("is_co_star", BooleanType()),
                    StructField("is_contingent", BooleanType()),
                    StructField("is_foreclosure", BooleanType()),
                    StructField("is_mls_rental", BooleanType()),
                    StructField("is_new_construction", BooleanType()),
                    StructField("is_new_listing", BooleanType()),
                    StructField("is_new_plan", BooleanType()),
                    StructField("is_office_standard_listing", BooleanType()),
                    StructField("is_pending", BooleanType()),
                    StructField("is_recently_sold", BooleanType()),
                    StructField("is_rental_community", BooleanType()),
                    StructField("is_rental_unit", BooleanType()),
                    StructField("is_short_sale", BooleanType()),
                    StructField("is_showcase", BooleanType()),
                    StructField("is_showcase_choice_enabled", BooleanType()),
                    StructField("is_turbo", BooleanType()),
                    StructField("lead_form_phone_required", BooleanType()),
                    StructField("presentation_status", StringType()),
                    StructField("price_change", LongType()),
                    StructField("price_reduced", BooleanType()), 
                    StructField("show_contact_a_lender_in_lead_form", BooleanType()),
                    StructField("show_veterans_united_in_lead_form", BooleanType()),
                    StructField("suppress_map_pin", BooleanType()),
                    StructField("suppress_phone_call_lead_event", BooleanType()), 
                ])
            ), 
            StructField("data_source_name", StringType()),
            StructField("fingerprint", LongType()),
            StructField("garage", StringType()),
            StructField("is_new_construction", BooleanType()),
            StructField("last_update", StringType()),
            StructField("list_date", StringType()),
            StructField("lead_forms", 
                StructType([
                    StructField("cashback_enabled", BooleanType()),
                    StructField("flip_the_market_enabled", BooleanType()), 
                    StructField("form",
                        StructType([
                            StructField("email",
                                StructType([
                                    StructField("minimum_character_count", LongType()),
                                    StructField("required", BooleanType())
                                ])
                            ),
                            StructField("message",
                                StructType([
                                    StructField("minimum_character_count", LongType()),
                                    StructField("required", BooleanType())
                                ])
                            ),
                            StructField("move_in_date",
                                StructType([
                                    StructField("default_date", StringType()),
                                    StructField("maximum_days_from_today", LongType()),
                                    StructField("minimum_days_from_today", LongType()),
                                    StructField("required", BooleanType())
                                ])
                            ),
                            StructField("name",
                                StructType([
                                    StructField("minimum_character_count", LongType()),
                                    StructField("required", BooleanType())
                                ])
                            ),
                            StructField("phone",
                                StructType([
                                    StructField("maximum_character_count", LongType()),
                                    StructField("minimum_character_count", LongType()),
                                    StructField("required", BooleanType())
                                ])
                            ),
                            StructField("show", BooleanType())
                        ])
                    ),
                    StructField("form_type", StringType()), 
                    StructField("is_lcm_enabled", BooleanType()), 
                    StructField("lead_type", StringType()), 
                    StructField("local_phone", StringType()),
                    StructField("local_phones",
                            StructType([
                                StructField("comm_console_mweb", StringType())
                            ]), 
                    ),
                    StructField("show_agent", BooleanType()), 
                    StructField("show_broker", BooleanType()), 
                    StructField("show_builder", BooleanType()), 
                    StructField("show_contact_a_lender", BooleanType()), 
                    StructField("show_management", BooleanType()), 
                    StructField("show_provider", BooleanType()), 
                    StructField("show_text_leads", BooleanType()), 
                    StructField("show_veterans_united", BooleanType()),
                    StructField("smarthome_enabled", BooleanType()), 
                ])
            ), 
            StructField("list_tracking", StringType()),
            StructField("listing_id", StringType()),
            StructField("listing_provider_url", 
                StructType([
                    StructField("level", StringType()), 
                    StructField("type", StringType()), 
                    StructField("url", StringType())
                ])
            ),
            StructField("lising_status", StringType()),
            StructField("lot_size", 
                StructType([
                    StructField("size", LongType()), 
                    StructField("units", StringType())
                ])
            ),
            StructField("mls", 
                StructType([
                    StructField("abbreviation", StringType()),
                    StructField("id", StringType()), 
                    StructField("name", StringType()),
                    StructField("plan_id", StringType()),
                    StructField("type", StringType())
                ])
            ),
            StructField("office", 
                StructType([
                    StructField("advertiser_id", LongType()), 
                    StructField("email", StringType()), 
                    StructField("href", StringType()), 
                    StructField("id", StringType()),
                    StructField("name", StringType()),
                    StructField("phones", 
                        ArrayType(StructType([
                            StructField("number", StringType()), 
                            StructField("primary", BooleanType()), 
                            StructField("type", StringType())
                        ]))
                    ),
                    StructField("photo", StructType([
                        StructField("href", StringType())
                    ]))
                ])
            ),   
            StructField("page_no", LongType()),
            StructField("photo_count", LongType()),
            StructField("photos", 
                ArrayType(StructType([
                    StructField("description", StringType()), 
                    StructField("href", StringType()), 
                    StructField("tags", 
                        ArrayType(StructType([
                            StructField("label", StringType()),
                            StructField("probability", DoubleType())
                        ]))
                    ),
                    StructField("title", StringType()), 
                    StructField("type", StringType()),                       
                ]))
            ),
            StructField("price", LongType()),
            StructField("price_reduced_date", StringType()),
            StructField("products", ArrayType(StringType())), 
            StructField("prop_status", StringType()),
            StructField("prop_type", StringType()),
            StructField("prop_sub_type", StringType()),
            StructField("property_id", StringType()),
            StructField("rank", LongType()),
            StructField("raw", 
                StructType([
                    StructField("status", StringType())
                ])
            ),
            StructField("rdc_app_url", StringType()),
            StructField("rdc_web_url", StringType()),
            StructField("sold_history", ArrayType(StringType())),
            StructField("source", StringType()), 
            StructField("tax_history", ArrayType(StringType())),
            StructField("thumbnail", StringType()),
            StructField("virtual_tour", 
                StructType([ 
                    StructField("href", StringType())
                ])
            ),
            StructField("year_built", LongType())
       ])
     )
   )
])
df = spark.read\
    .schema(properties_df_schema) \
    .option("badRecordsPath", "/tmp/malformed_records")\
    .format(file_type) \
    .load(file_location)

# COMMAND ----------

# DBTITLE 1,Build, clean, and cache Property DataFrame
from pyspark.sql.functions import explode, col, struct, concat_ws

properties_unexpanded = df.select(explode(df.properties).alias("properties"))
properties_nested = properties_unexpanded.select("properties.*")
properties_clean = properties_nested.selectExpr(
                                        "address",
                                        "explode(agents) as agent",
                                        "baths",
                                        "baths_full",
                                        "baths_half",
                                        "beds",
                                        "building_size.size as building_size",
                                        "building_size.units as building_units",
                                        "client_display_flags.flip_the_market_enabled",
                                        "client_display_flags.has_open_house",
                                        "client_display_flags.has_specials",
                                        "client_display_flags.is_apartmentlist",
                                        "client_display_flags.is_co_broke_email",
                                        "client_display_flags.is_co_broke_phone",
                                        "client_display_flags.is_co_star",
                                        "client_display_flags.is_contingent",
                                        "client_display_flags.is_foreclosure",
                                        "client_display_flags.is_mls_rental",
                                        "client_display_flags.is_new_construction",
                                        "client_display_flags.is_new_listing",
                                        "client_display_flags.is_new_plan",
                                        "client_display_flags.is_office_standard_listing",
                                        "client_display_flags.is_pending",
                                        "client_display_flags.is_recently_sold",
                                        "client_display_flags.is_rental_community",
                                        "client_display_flags.is_rental_unit",
                                        "client_display_flags.is_short_sale",
                                        "client_display_flags.is_showcase",
                                        "client_display_flags.is_showcase_choice_enabled",
                                        "client_display_flags.is_turbo",
                                        "client_display_flags.lead_form_phone_required",
                                        "client_display_flags.presentation_status",
                                        "client_display_flags.price_change",
                                        "client_display_flags.price_reduced",
                                        "client_display_flags.show_contact_a_lender_in_lead_form",
                                        "client_display_flags.show_veterans_united_in_lead_form",
                                        "client_display_flags.suppress_map_pin",
                                        "client_display_flags.suppress_phone_call_lead_event",
                                        "data_source_name",
                                        "garage",
                                        "last_update",
                                        "list_date",
                                        "list_tracking",
                                        "listing_id",
                                        "lot_size.size as lot_size",
                                        "lot_size.units as lot_units",
                                        "mls",
                                        "office",
                                        "page_no",
                                        "photo_count",
                                        "photos",
                                        "price",
                                        "price_reduced_date",
                                        "products",
                                        "prop_status",
                                        "prop_type",
                                        "prop_sub_type",
                                        "property_id",
                                        "rank",
                                        "raw.status as raw_status",
                                        "rdc_app_url",
                                        "rdc_web_url",
                                        "sold_history",
                                        "source",
                                        "tax_history",
                                        "thumbnail",
                                        "virtual_tour.href as virtual_tour_href",
                                        "year_built")\
                                    .withColumn("address", 
                                                 struct(*[
                                                  concat_ws('-', col("address.line"), col("address.city"), col("address.county"), col("address.postal_code")).alias("id"),
                                                  col("address.city"),
                                                  col("address.country"),
                                                  col("address.county"),
                                                  col("address.county_needed_for_uniq"),
                                                  col("address.fips_code"),
                                                  col("address.lat"),
                                                  col("address.line"),
                                                  col("address.lon"),
                                                  col("address.neighborhood_name"),
                                                  col("address.neighborhoods"),
                                                  col("address.postal_code"),
                                                  col("address.state"),
                                                  col("address.state_code"),
                                                  col("address.street"),
                                                  col("address.street_number"),
                                                  col("address.street_suffix"),
                                                  col("address.time_zone"),
                                                  col("address.unit")
                                                 ]))
properties_clean.cache()

# COMMAND ----------

# DBTITLE 1,Scala UDF to create Neighborhood_Id column on Address Dataframe
# MAGIC %scala
# MAGIC import scala.collection.mutable.ListBuffer
# MAGIC 
# MAGIC val pipeJoin = (row: Seq[Row]) => {
# MAGIC   if (row == null) null
# MAGIC   else{
# MAGIC     var neighborhood_ids = new ListBuffer[String]()
# MAGIC     row.foreach(el => neighborhood_ids += el.getAs[String]("id"))
# MAGIC     if (neighborhood_ids.size > 0) neighborhood_ids.mkString("|")
# MAGIC     else null
# MAGIC   }  
# MAGIC }
# MAGIC 
# MAGIC var pipeJoinUdf = udf(pipeJoin)
# MAGIC spark.udf.register("pipeJoin", pipeJoinUdf)

# COMMAND ----------

# DBTITLE 1,Build and clean nested Address DataFrame
addresses = properties_clean.selectExpr(
                                    "address.city",
                                    "address.country",
                                    "address.county",
                                    "address.county_needed_for_uniq",
                                    "address.fips_code",
                                    "address.lat as latitude",
                                    "address.line as address_line",
                                    "address.lon as longitude",
                                    "address.neighborhood_name",
                                    "pipeJoin(address.neighborhoods) as neighborhoods",
                                    "address.postal_code",
                                    "address.state",
                                    "address.state_code",
                                    "address.street",
                                    "address.street_number",
                                    "address.street_suffix",
                                    "address.time_zone",
                                    "address.unit",
                                    "address.id as address_id")\
                            .distinct()\
                            .na.drop(subset=["address_line", "city", "county", "postal_code"])

# COMMAND ----------

# DBTITLE 1,Build and clean double nested Neighborhoods DataFrame
neighborhoods_exploded = properties_clean.selectExpr("explode(address.neighborhoods) as neighborhood")
neighborhoods = neighborhoods_exploded.selectExpr(
                                                "neighborhood.city",
                                                "neighborhood.id as neighborhood_id",
                                                "neighborhood.level",
                                                "neighborhood.name",
                                                "neighborhood.state_code")\
                                      .distinct()\
                                      .na.drop(subset=["neighborhood_id", "city", "name", "state_code"]) 

# COMMAND ----------

# DBTITLE 1,Build and clean nested Offices
offices = properties_clean.selectExpr(
                               "office.advertiser_id",
                               "office.email",
                               "office.href as website",
                               "office.id as office_id",
                               "office.name",
                               "office.phones",
                               "office.photo.href as photo_href")\
                           .distinct()\
                           .na.drop("all")\
                           .drop("phones")

# COMMAND ----------

# DBTITLE 1,Build and clean double nested Phones DataFrame
phones_exploded = properties_clean.selectExpr("office.id as office_id", "explode(office.phones) as phone")
phones = phones_exploded.selectExpr("office_id", "phone.number", "phone.primary", "phone.type").na.drop(subset=["number"]).distinct()

# COMMAND ----------

# DBTITLE 1,Build and clean nested Agents DataFrame
agents = properties_clean.selectExpr(
                            "agent.advertiser_id", 
                            "agent.id as agent_id", 
                            "agent.name", 
                            "agent.photo.href as photo_href", 
                            "agent.primary")\
                         .na.drop(subset=["agent_id", "name"])\
                         .distinct()

# COMMAND ----------

# DBTITLE 1,Build and clean nested MLS DataFrame
mls = properties_clean.selectExpr("mls.abbreviation", 
                                  "mls.id as mls_id",
                                  "mls.name",
                                  "mls.plan_id",
                                  "mls.type")\
                      .na.drop(subset=["mls_id"])\
                      .distinct()

# COMMAND ----------

# DBTITLE 1,Build and clean nested one-to-many Photos DataFrame
photos_exploded = properties_clean.selectExpr("property_id", "explode(photos) as photo").distinct()
photos = photos_exploded.filter("size(photo.tags) > 0")\
                        .selectExpr(
                            "property_id",
                            "photo.description", 
                            "photo.href", 
                            "photo.tags[0].label as tag", 
                            "photo.title",
                            "photo.type")\
                        .na.drop(subset=["property_id", "href"])\
                        .distinct()

# COMMAND ----------

# DBTITLE 1,Map foreign keys
remapped_property_columns = ["address", "agent", "mls", "office"]

for column in remapped_property_columns:
    properties_clean = properties_clean.withColumn(column, col(column).id)\
                                       .withColumnRenamed(column, column + "_id")

properties = properties_clean.drop("photos")
properties_clean.unpersist()

# COMMAND ----------

# DBTITLE 1,Upsert functions
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.utils import AnalysisException

def merge_into_or_create_delta(database, table_name, df, schema, primary_keys, opts): 
    df = df.dropDuplicates(primary_keys)
    
    if not df._jdf.isEmpty():
        spark = SparkSession.getActiveSession()
        schema_bound_df =  spark.createDataFrame(df.rdd, schema=schema)
        if DeltaTable.isDeltaTable(spark, opts["path"]):       
            tbl = DeltaTable.forPath(spark, opts["path"])  
            dest_name = "dests"
            update_name = "updates"
            merge_condition = " and ".join([f"{dest_name}.{col} <=> {update_name}.{col}" for col in primary_keys])
            tbl.alias(dest_name).merge(schema_bound_df.alias(update_name), merge_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 
        else:
            full_table_name = "{}.{}".format(database, table_name) 
            schema_bound_df.write.format("delta").options(**opts).saveAsTable(full_table_name)

# COMMAND ----------

# DBTITLE 1,Staging config mappings
from pyspark.sql.types import *

staging_schemas= { 
    "addresses" : StructType([
        StructField("city", StringType()),
        StructField("country", StringType()), 
        StructField("county", StringType()),
        StructField("county_needed_for_uniq", BooleanType()),
        StructField("fips_code", StringType()), 
        StructField("latitude", DoubleType()),
        StructField("address_line", StringType()), 
        StructField("longitude", DoubleType()),
        StructField("neighborhood_name", StringType()),
        StructField("neighborhoods", StringType()),
        StructField("postal_code", StringType()),
        StructField("state", StringType()), 
        StructField("state_code", StringType()),
        StructField("street", StringType()), 
        StructField("street_number", StringType()), 
        StructField("street_suffix", StringType()),
        StructField("time_zone", StringType()), 
        StructField("unit", StringType()), 
        StructField("address_id", StringType()),
    ]),
    "agents": StructType([
        StructField("advertiser_id", StringType()), 
        StructField("agent_id", StringType()),
        StructField("name", StringType()), 
        StructField("photo_href", StringType()), 
        StructField("primary", BooleanType()), 

    ]), 
    "offices": StructType([
        StructField("advertiser_id", LongType()), 
        StructField("email", StringType()),
        StructField("website", StringType()),
        StructField("office_id", StringType()),
        StructField("name", StringType()), 
        StructField("photo_href", StringType())
    ]),
    "mls": StructType([
        StructField("abbreviation", StringType()),
        StructField("mls_id", StringType()),
        StructField("name", StringType()),
        StructField("plan_id", StringType()),
        StructField("type", StringType())
    ]), 
    "neighborhoods" : StructType([
        StructField("city", StringType()),
        StructField("neighborhood_id", StringType()), 
        StructField("level", StringType()),
        StructField("name", StringType()),
        StructField("state_code", StringType())
    ]),
    "photos": StructType([
        StructField("property_id", StringType()),
        StructField("description", StringType()),
        StructField("href", StringType()),
        StructField("title", StringType()),
        StructField("type", StringType()),
        StructField("tag", StringType())
    ]),
    "phones": StructType([
        StructField("office_id", StringType()), 
        StructField("number", StringType()),
        StructField("primary", BooleanType()),
        StructField("office", StringType())
    ]),
    "properties": StructType([
        StructField("address_id", StringType()), 
        StructField("agent_id", StringType()),
        StructField("baths", LongType()), 
        StructField("baths_full", LongType()),
        StructField("baths_half", LongType()),
        StructField("beds", IntegerType()),
        StructField("building_size", LongType()), 
        StructField("building_units", StringType()),
        StructField("flip_the_market_enabled", BooleanType()), 
        StructField("has_open_house", BooleanType()), 
        StructField("has_specials", BooleanType()), 
        StructField("is_apartmentlist", BooleanType()), 
        StructField("is_co_broke_email", BooleanType()),
        StructField("is_co_broke_phone", BooleanType()),
        StructField("is_co_star", BooleanType()), 
        StructField("is_contingent", BooleanType()), 
        StructField("is_foreclosure", BooleanType()),
        StructField("is_mls_rental", BooleanType()), 
        StructField("is_new_construction", BooleanType()),
        StructField("is_new_listing", BooleanType()),
        StructField("is_new_plan", BooleanType()),
        StructField("is_office_standard_listing", BooleanType()),
        StructField("is_pending", BooleanType()), 
        StructField("is_recently_sold", BooleanType()),
        StructField("is_rental_community", BooleanType()), 
        StructField("is_rental_unit", BooleanType()), 
        StructField("is_short_sale", BooleanType()),
        StructField("is_showcase", BooleanType()),
        StructField("is_showcase_choice_enabled", BooleanType()),
        StructField("is_turbo", BooleanType()),
        StructField("lead_form_phone_required", BooleanType()),
        StructField("presentation_status", StringType()),
        StructField("price_change", LongType()),
        StructField("price_reduced", BooleanType()), 
        StructField("show_contact_a_lender_in_lead_form", BooleanType()),
        StructField("show_veterans_united_in_lead_form", BooleanType()),
        StructField("suppress_map_pin", BooleanType()),  
        StructField("supress_phone_call_lead_event", BooleanType()), 
        StructField("data_source_name", StringType()),
        StructField("garage", StringType()),
        StructField("last_update", StringType()),
        StructField("list_date", StringType()),
        StructField("list_tracking", StringType()),
        StructField("listing_id", StringType()),
        StructField("lot_size", LongType()),
        StructField("lot_units", StringType()),
        StructField("mls_id", StringType()),
        StructField("office_id", StringType()),            
        StructField("page_no", LongType()),
        StructField("photo_count", LongType()),
        StructField("price", LongType()),
        StructField("price_reduced_date", StringType()),
        StructField("products", ArrayType(StringType())), 
        StructField("prop_status", StringType()),
        StructField("prop_type", StringType()),
        StructField("prop_sub_type", StringType()), 
        StructField("property_id", StringType()),
        StructField("rank", LongType()),
        StructField("raw_status", StringType()),
        StructField("rdc_app_url", StringType()),
        StructField("rdc_web_url", StringType()),
        StructField("sold_history", ArrayType(StringType())),
        StructField("source", StringType()),
        StructField("tax_history", ArrayType(StringType())),
        StructField("thumbnail", StringType()),
        StructField("virtual_tour_href", StringType()),
        StructField("year_built", LongType())
    ])
}

primary_keys_mapping = {
    "addresses": ["address_id"], 
    "agents": ["agent_id"],
    "offices": ["office_id", "advertiser_id"],
    "neighborhoods": ["neighborhood_id"],
    "mls": ["mls_id"],
    "photos": ["property_id", "href"],
    "phones": ["office_id", "number"],
    "properties": ["agent_id", "office_id", "property_id", "list_date", "price", "price_reduced_date"]
}

# COMMAND ----------

# DBTITLE 1,Create Staging database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS staging;

# COMMAND ----------

# DBTITLE 1,Load batch into Staging database
staging_load_map = {
    "addresses": {
        "data": addresses,
        "schema": staging_schemas["addresses"],
        "keys": primary_keys_mapping["addresses"],
        "path":  "/tmp/staging/addresses"
    },
    "agents": {
        "data": agents,
        "schema": staging_schemas["agents"],
        "keys": primary_keys_mapping["agents"],
        "path":  "/tmp/staging/agents"
    },
    "offices": {
        "data": offices,
        "schema": staging_schemas["offices"],
        "keys": primary_keys_mapping["offices"],
        "path":  "/tmp/staging/offices"
    },
    "mls": {
        "data": mls,
        "schema": staging_schemas["mls"],
        "keys": primary_keys_mapping["mls"],
        "path":  "/tmp/staging/mls"
    },
    "neighborhoods": {
        "data": neighborhoods,
        "schema": staging_schemas["neighborhoods"],
        "keys": primary_keys_mapping["neighborhoods"],
        "path":  "/tmp/staging/neighborhoods"
    },
    "photos": {
        "data": photos,
        "schema": staging_schemas["photos"],
        "keys": primary_keys_mapping["photos"],
        "path":  "/tmp/staging/photos"
    },
    "phones": {
        "data": phones,
        "schema": staging_schemas["phones"],
        "keys": primary_keys_mapping["phones"],
        "path":  "/tmp/staging/phones"
    },
    "properties" : {
        "data" : properties,
        "schema": staging_schemas["properties"],
        "keys": primary_keys_mapping["properties"],
        "path": "/tmp/staging/properties"
    }
}

for staging_table, config in staging_load_map.items():
    merge_into_or_create_delta("staging", 
                               staging_table, 
                               config["data"], 
                               config["schema"], 
                               config["keys"], 
                               {
                                 "path":config["path"],
                               })
