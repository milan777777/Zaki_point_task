from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col,hash,expr,array,concat_ws,concat,when,array_except,lit,lpad
from pyspark.sql.types import ArrayType, IntegerType, ShortType

def scrub_nrpr(output_folder,pro,etl):
    spark = etl.spark
    df_pyspark = spark.read.option('multiline','true').json(output_folder)
    
    df_pyspark.printSchema()
     
    np_data = (
        df_pyspark.selectExpr("*", "explode(in_network) as net").drop("in_network")
        .select("*", "net.*").drop("net")
        .selectExpr("*", "explode(negotiated_rates) as rates").drop("negotiated_rates")
        .selectExpr("*", "explode(rates.provider_groups) as id").drop("provider_groups")
        .selectExpr("*", "explode(id.npi) as npi", "id.tin.type as tin_type", "id.tin.value as tin").drop("id")
        .selectExpr("*", "explode(rates.negotiated_prices) as prices").drop("rates")
        .select("*", "prices.*").drop("prices")
    )

    provider_cleaned = np_data.withColumn('tin', expr("REPLACE(tin, '-', '')"))
    provider_replaced = provider_cleaned.withColumn("tin_type",
                    when(col("tin_type") == "ein", 1)
                    .when(col("tin_type") == "npi", 2))
    
    df_combine = provider_replaced.withColumn("provider_group_id",concat("npi", "tin"))
    df_hash = df_combine.withColumn('provider_group_id',hash("provider_group_id"))
    df_nr_neww = df_combine.drop('npi', 'tin_type', 'tin')
    
    in_net = df_nr_neww.select(
    "billing_code",
    "billing_code_type",
    "negotiation_arrangement",
    "provider_group_id",
    "billing_class",
    "billing_code_modifier",
    "negotiated_rate",
    "negotiated_type",
    "service_code"
    )

    np_data = (in_net.filter(in_net.billing_code.isNotNull() & (in_net.billing_code != ""))
            .withColumn("service_code",col("service_code").cast(ArrayType(IntegerType())))
    )


    network_nrpr = "output/data.parquet"
    np_data.write.mode("Overwrite").parquet(network_nrpr)

    # for mrf pr
    df1_pyspark = spark.read.parquet(pro)
    provider_table = df_hash.selectExpr( 'npi',
            "tin_type",
            "tin",
            "provider_group_id")

    remove_ = provider_table.withColumn('tin', expr("replace(tin,'-','')"))
    df_type = remove_.withColumn('tin_type',
            when((col('tin_type')== 'ein'), 1).when((col('tin_type')== 'npi'), 2)
    )
    provider_rep = df_type.withColumn("tin_type", col("tin_type").cast(ShortType()))


    # for new provider table
    df_dropping = df1_pyspark.drop('prv_fax','provider_name_prefix_text','prv_type_desc')
    df_new = df_dropping.withColumn('prv_type_code',
            when((col('prv_type_code')== 'P'), 1).when((col('prv_type_code')== 'F'), 2)
    )
    df_cast = df_new.withColumn("prv_type_code",col("prv_type_code").cast(IntegerType()))
    df_merge = df_cast.withColumn("full_name",concat_ws(" ","provider_first_name", "provider_middle_name","provider_last_name"))
    df_merge1=df_merge.drop("provider_first_name","provider_last_name","provider_middle_name")

    df_new1 = df_merge1.select(
    "*",  
    col("loc.lat").alias("latitude"),
    col("loc.lon").alias("longitude")
    )
    df_new2 = df_new1.drop('loc')

    df_array = df_new2.withColumn("taxonomy",array(col("prv_taxonomy_1_code"),col("prv_taxonomy_2_code"),col("prv_taxonomy_3_code")))
    df_array1=df_array.drop("prv_taxonomy_1_code","prv_taxonomy_2_code","prv_taxonomy_3_code")
    df_array2 = df_array1.withColumn("prv_specialty",array(col("prv_specialty_1_desc"),col("prv_specialty_2_desc"),col("prv_specialty_3_desc")))
    df_array3=df_array2.drop("prv_specialty_1_desc","prv_specialty_2_desc","prv_specialty_3_desc")

    # join provider table
    df_join = provider_rep.join(df_array3,how="inner",on=["npi","tin"])
    providernr_pr = "output/prov.parquet"
    df_join.write.mode("Overwrite").parquet(providernr_pr)
    
        
    # new csv file
    df_bil_code = spark.read.option('header','True').csv('/home/milan-thapa/Desktop/Zaki_point_task/files/billing_taxonomy_list (2).csv')
    df_leftpad = df_bil_code.withColumn("billing_code", lpad(col("billing_code"), 5, "0"))
    df_rate2 = df_leftpad.drop('_c4','_c5','_c6')
    df_join = df_rate2.select(
    "billing_code",
    "taxonomy_list"
    )
    df_newrtab = df_nr_neww.join(df_join,on="billing_code",how="inner")
    


    df_newrtab.printSchema()
    
    return np_data,provider_rep,df_array3,df_rate2,df_join



    
    



    


