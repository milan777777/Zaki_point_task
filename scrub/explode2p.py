#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


from pyspark.sql.functions import explode,col,expr,array,when
from pyspark.sql.types import ArrayType, IntegerType, ShortType


# In[3]:


spark = SparkSession.builder.appName("task").getOrCreate()


# In[4]:


spark


# In[5]:


df_pyspark = spark.read.option('multiline','true').json('provider.json')


# In[6]:


df_pyspark.printSchema()


# In[7]:


provider_group = df_pyspark.withColumn("new_provider", explode("provider_groups"))
provider_new = provider_group.withColumn("npi",explode("new_provider.npi"))
provider_again = provider_new.select(
    "provider_group_id",
    col("npi").alias("npi"),
    col("new_provider.tin.type").alias("tin_type"),
    col("new_provider.tin.value").alias("tin")
)      


# In[8]:


provider_again.printSchema()


# In[9]:


provider_again.show()


# In[10]:


provider_tin=provider_again.withColumn("tin_type",when(col("tin_type")=="ein",1)
                                      .when(col("tin_type")=="npi",2))
provider_final = provider_tin.withColumn("tin",expr("REPLACE(tin,'-','')"))


# In[11]:


provider_final.show()


# In[12]:


provider_cast = provider_final.withColumn("tin_type", col("tin_type").cast(ShortType()))


# In[13]:


provider_cast.printSchema()


# In[14]:


provider_cast.write.parquet('provider_output.parquet')


# In[ ]:




