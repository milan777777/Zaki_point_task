#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession




# In[2]:


from pyspark.sql.functions import explode,col,expr,array,when
from pyspark.sql.types import ArrayType, IntegerType, ShortType


# In[3]:


spark = SparkSession.builder.appName("task1")
spark = SparkSession.builder.appName('ETL').config("spark.driver.memory", "4g").getOrCreate()


# In[4]:


spark


# In[5]:


df_pyspark = spark.read.option('multiline','true').json('rate.json')


# In[6]:


df_pyspark.printSchema()


# In[7]:


negotiated_rates_df = df_pyspark.withColumn("row1",explode("negotiated_rates"))
negotiated_new = negotiated_rates_df.withColumn("id",explode("row1.provider_references"))
negotiated_prices_df = negotiated_new.withColumn("row2",explode("row1.negotiated_prices"))

rate_df = negotiated_prices_df.select("billing_code","billing_code_type","negotiation_arrangement",
                                      col("row2.billing_code_modifier").alias("billing_code_modifier"),
                                      col("row2.billing_class").alias("billing_class"),
                                      col("row2.negotiated_rate").alias("negotiated_rate"),
                                      col("row2.service_code").alias("service_code"),
                                      col("id").alias("provider_group_id"),
                                      col("row2.negotiated_type").alias("negotiated_type"))
rate_df.printSchema()


# In[8]:


rate_df.show(10)


# In[9]:


rate_df.printSchema()


# In[10]:


net_cast = rate_df.withColumn("service_code", col("service_code").cast(ArrayType(IntegerType())))


# In[11]:


net_cast.printSchema()


# In[12]:


net_cast.write.parquet('net_output.parquet')


# In[ ]:




