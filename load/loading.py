import psycopg2


def load_file(rate_path,provider_path,etl):
    spark =etl.spark

    port = etl.port
    dbname = etl.dbname
    user = etl.user
    password = etl.password
    host = etl.host

    spark




    network_data = spark.read.parquet(rate_path)
    provider_data= spark.read.parquet(provider_path)
    provider_data.printSchema()





    conn = psycopg2.connect(
        dbname = dbname,
        user = user,
        password = password,
        host = host,
        port = port
    )
    # conn.autocommit = True

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
    connection_properties = {
        "user": user,
        "password": password,
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