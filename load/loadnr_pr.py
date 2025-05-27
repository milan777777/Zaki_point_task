import psycopg2


def load_import(network_nrpr,providernr_pr,df_rate2,logger,etl):
    logger.info("Starting load process")
    spark =etl.spark
    port = etl.port
    dbname = etl.dbname
    user = etl.user
    password = etl.password
    host = etl.host

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
    jdbc_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
   
    connection = psycopg2.connect(
        dbname = dbname,
        user = user,
        password = password,
        host = host,
        port = port
        )
    cursor = connection.cursor()
   
    df_join = """
    DROP TABLE IF EXISTS pr_table;
    CREATE TABLE pr_table(
        provider_group_id INT,
        npi BIGINT,
        tin_type SMALLINT,
        tin TEXT,
        prv_city VARCHAR,
        prv_phone VARCHAR,
        prv_state VARCHAR,
        prv_street_1 VARCHAR,
        prv_type_code SMALLINT,
        prv_zip VARCHAR,
        full_name VARCHAR,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        taxonomy TEXT[],
        prv_specialty TEXT[],
        geom GEOGRAPHY GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography)STORED
    );
    """

    cursor.execute(df_join)
    connection.commit()

    df_newrtab= """
    DROP TABLE IF EXISTS nr_table;
    CREATE TABLE nr_table(
        billing_code VARCHAR(5),
        billing_code_type VARCHAR,
        negotiation_arrangement VARCHAR,    
        provider_group_id INT,     
        billing_class VARCHAR,
        billing_code_modifier TEXT[],
        negotiated_rate DOUBLE PRECISION,    
        negotiated_type VARCHAR,
        service_code INTEGER[],
        taxonomy_list TEXT[]
    );
    """
    cursor.execute(df_newrtab)
    connection.commit()

    cursor.execute("CREATE SCHEMA IF NOT EXISTS taxonomy;")
    connection.commit()

    bill_table = """
    DROP TABLE IF EXISTS taxonomy.billing_taxonomy;
    CREATE TABLE IF NOT EXISTS taxonomy.billing_taxonomy (
        billing_code VARCHAR(5),
        billing_code_type VARCHAR(10),
        billing_description VARCHAR,
        taxonomy_list TEXT[]
    );
    """
    cursor.execute(bill_table)
    connection.commit()

    network_data = spark.read.parquet(network_nrpr)
    provider_data = spark.read.parquet(providernr_pr)

    provider_data.write.jdbc(url=jdbc_url,table="pr_table",mode="append", properties=jdbc_properties)
    network_data.write.jdbc(url=jdbc_url,table="nr_table",mode="append", properties=jdbc_properties)
    df_rate2.write.jdbc(url=jdbc_url,table="taxonomy.billing_taxonomy",mode="append", properties=jdbc_properties)

    logger.info("Successfully load to postgresql")
    cursor.close()
    connection.close()