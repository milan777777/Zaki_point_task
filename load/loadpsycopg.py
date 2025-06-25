#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


import psycopg2


# In[3]:


spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
    .getOrCreate()


# In[4]:


df = spark.read.parquet("/home/milan-thapa/Desktop/task 3/provider_output.parquet")
df1=spark.read.parquet("/home/milan-thapa/Desktop/task 3/net_output.parquet")


# In[5]:


conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="admin",
    host="localhost",
    port=5432
)


# In[6]:


jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
connection_properties = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}


# In[7]:


df1.printSchema()


# In[8]:


cur = conn.cursor()
create_table_query = """
DROP TABLE IF EXISTS in_network_data;
CREATE TABLE IF NOT EXISTS in_network_data (
    billing_code TEXT,
    billing_code_type TEXT,
    negotiation_arrangement TEXT,
    billing_code_modifier TEXT[],
    billing_class TEXT,
    negotiated_rate DOUBLE PRECISION,
    service_code INTEGER[],
    provider_group_id BIGINT,
    negotiated_type TEXT
);
"""

cur.execute(create_table_query)
conn.commit()


# In[9]:


df1.write.jdbc(url=jdbc_url,table="in_network_data",mode="append", properties=connection_properties)
conn.commit()


# In[10]:


# cur.close()
# conn.close()


# In[11]:


# dfa=spark.read.parquet("provider_output.parquet")
# dfa.show(20)


# In[12]:


cursor = conn.cursor()
provider_table = """
CREATE TABLE IF NOT EXISTS provider_table(
    provider_group_id BIGINT,
    npi BIGINT,
    tin_type SMALLINT,
    tin TEXT
);
"""
cursor.execute(provider_table)
conn.commit()


# In[13]:


df.write.jdbc(url=jdbc_url,table="provider_table",mode="append", properties=connection_properties)
conn.commit()


# In[ ]:




