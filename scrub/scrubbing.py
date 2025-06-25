from pyspark.sql.functions import explode,col,expr,when,concat_ws,array,array_except,lit
from pyspark.sql.types import ArrayType, IntegerType, ShortType, LongType
import yaml


def scrub_import(in_path,prov_path,provider_detail,etl,logger):
    logger.info("Starting transform process")
    spark = etl.spark

    rate_file = spark.read.json(in_path) 
    provider_file = spark.read.json(prov_path)
    pro_file = spark.read.json(provider_detail)

    rate_file.printSchema() 
    provider_file.printSchema()


    provider_df = provider_file.withColumn("provider", explode("provider_groups"))
    provider_npi = provider_df.withColumn("provider_npi", explode("provider.npi"))

    provider_flat = provider_npi.select( 
        "provider_group_id",
        col("provider_npi").alias("npi"), 
        col("provider.tin.type").alias("tin_type"),
        col("provider.tin.value").alias("tin")
    )
 
    provider_replaced = provider_flat.withColumn("tin_type",
                        when(col("tin_type") == "ein", 1)
                        .when(col("tin_type") == "npi", 2))
    provider_cleaned = provider_replaced.withColumn('tin', expr("REPLACE(tin, '-', '')"))


    provider_prov = provider_cleaned.withColumn("provider_group_id", col("provider_group_id").cast(IntegerType()))
    provider_cast = provider_prov.withColumn("tin_type", col("tin_type").cast(ShortType()))

   
    rate_df = rate_file.withColumn("rates", explode("negotiated_rates"))
    id_df = rate_df.withColumn("provider_group_id",explode("rates.provider_references"))
    price_df = id_df.withColumn("prices",explode("rates.negotiated_prices"))

    rate_flat = price_df.select( 
        "billing_code",
        "billing_code_type",
        "negotiation_arrangement",
        col("provider_group_id").alias("provider_group_id"),    
        col("prices.billing_class").alias("billing_class"),
        col("prices.billing_code_modifier").alias("billing_code_modifier"),
        col("prices.negotiated_rate").alias("negotiated_rate"),
        col("prices.negotiated_type").alias("negotiated_type"),
        col("prices.service_code").alias("service_code")
    )


    rate_filtered = rate_flat.filter(rate_flat.billing_code.isNotNull() & (rate_flat.billing_code != ""))
    rate_filtered.show()
     
  
    rate_filtered.filter(rate_filtered.billing_code_modifier.isNotNull()).count()
    
  
    rate_prov = rate_filtered.withColumn("provider_group_id", col("provider_group_id").cast(IntegerType()))
    rate_cast = rate_prov.withColumn("service_code",col("service_code").cast(ArrayType(IntegerType())))

    # new provider 

    pro_ = pro_file.select("*",col('loc.lat').alias('lat'), 
                            col('loc.lon').alias('lon') )

    ploc_ = pro_.drop("loc")

    p_concat = ploc_.withColumn("provider_full_name", concat_ws(" ", col("provider_first_name"), col("provider_middle_name"), col("provider_last_name")))

    
    p_merge = p_concat.withColumn("prv_taxonomy", array("prv_taxonomy_1_code", "prv_taxonomy_2_code", "prv_taxonomy_3_code")) \
                    .withColumn("prv_specialty", array("prv_specialty_1_desc", "prv_specialty_2_desc", "prv_specialty_3_desc"))

   
    p_drop = p_merge.drop("prv_fax", "prv_type_desc", "provider_first_name", "provider_last_name", "provider_middle_name", "provider_name_prefix_text",
                        "prv_taxonomy_1_code", "prv_taxonomy_2_code", "prv_taxonomy_3_code",
                        "prv_specialty_1_desc", "prv_specialty_2_desc", "prv_specialty_3_desc")

    pmap_ = p_drop.withColumn("prv_type_code",
                            when(col("prv_type_code") == "P", 1)
                            .when(col("prv_type_code") == "F", 2))
    
    p_cast = pmap_.withColumn("prv_type_code", col("prv_type_code").cast(IntegerType())) \
                .withColumn("npi", col("npi").cast(LongType()))
    
    _pro_table = provider_cast.join(p_cast,on="npi",how="inner")
    _pro_table.show() 

    rate_cast.printSchema()   
    provider_cast.printSchema()

    rate_path = "output_file/rate_data.parquet"
    provider_path = "output_file/provider_data.parquet"

    rate_cast.write.mode("overwrite").parquet(rate_path)
    _pro_table.write.mode("overwrite").parquet(provider_path)

    return rate_path,provider_path
