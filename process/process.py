from pyspark.sql.functions import size,array_intersect,col

def process(df_nr_neww,provider_rep,df_array3,df_join,logger):
    df_newrtab = df_nr_neww.join(df_join,on="billing_code",how="inner")
    df_pr_join = provider_rep.join(df_array3,how="inner",on=["npi","tin"])

    df_newrtab.printSchema()
    df_pr_join.printSchema()

    nrpr = df_newrtab.join(df_pr_join,on="provider_group_id",how="inner")

    specialized_filter = nrpr.filter(size(array_intersect(col("taxonomy"), col("taxonomy_list"))) > 0)
    specialized_filter.show(5)
     

    network_nrpr = "output/data.parquet"
    providernr_pr = "output/prov.parquet"



    df_newrtab.write.mode("overwrite").parquet(network_nrpr)
    df_pr_join.write.mode("overwrite").parquet(providernr_pr)

    
    logger.info("File transformed successfully")
    return network_nrpr,providernr_pr



