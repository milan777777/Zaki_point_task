from pyspark.sql import SparkSession
import psycopg2


def load_file(rate_path,provider_path):

    spark = SparkSession.builder.appName('task').getOrCreate()



    spark




    network_data = spark.read.parquet(rate_path)
    provider_data= spark.read.parquet(provider_path)
    provider_data.printSchema()





    conn = psycopg2.connect(
        dbname="my_pgdb",
        user="postgres",
        password="admin",
        host="localhost",
        port=5432
    )
    # conn.autocommit = True

    jdbc_url = "jdbc:postgresql://localhost:5432/my_pgdb"
    connection_properties = {
        "user": "postgres",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }




    cur = conn.cursor()
    provider_ref = """
    CREATE TABLE IF NOT EXISTS provider_ref (
        provider_group_id BIGINT,
        npi BIGINT,
        tin_type SMALLINT,
        tin TEXT
    );
    """
    cur.execute(provider_ref)

    conn.commit()

    provider_data.write.jdbc(url=jdbc_url,table="provider_new",mode="append", properties=connection_properties)


    cur = conn.cursor()

    network_new = """

    CREATE TABLE IF NOT EXISTS network_new (
        billing_code TEXT,
        billing_code_type TEXT,
        negotiation_arrangement TEXT,
        provider_group_id BIGINT,
        billing_class TEXT,
        billing_code_modifier TEXT[],
        negotiated_rate DOUBLE PRECISION,
        negotiated_type TEXT,
        service_code INTEGER[]
    );
    """
    cur.execute(network_new)

    conn.commit()




    network_data.write.jdbc(url=jdbc_url,table="network_new",mode="append", properties=connection_properties)




    cur.close()
    conn.close()