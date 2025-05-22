from pyspark.sql import SparkSession
# Initialize Spark
spark = SparkSession.builder \
    .appName("Dynamic CDC Pipeline") \
    .getOrCreate()

# PostgreSQL JDBC config
jdbc_url = "jdbc:postgresql://18.170.23.150:5432/testdb"
properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}
spark = SparkSession.builder \
    .appName("CDC Job") \
    .config("spark.jars", "/path/to/postgresql-<version>.jar") \
    .getOrCreate()


# Step 1: Load CDC metadata from PostgreSQL
cdc_meta_df = spark.read.jdbc(url=jdbc_url, table="cdc_metadata", properties=properties)
active_tables = cdc_meta_df.filter("is_active = true").collect()

for row in active_tables:
    table = row['source_table']
    pk = row['primary_key']
    tracking_col = row['tracking_column']
    target_path = row['s3://cdcimplementation/cdc1'] if row['s3://cdcimplementation/cdc1'] else f"'s3://cdcimplementation'/{table}/"

    # Step 2: Load changed data from source table
    #query = f"(SELECT * FROM cdc1 WHERE {tracking_col} > '{last_sync}') AS t"
    query = f"(SELECT * FROM {table}) AS temp"
    full_df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

    # Write to Parquet (overwrite mode for full load)
    full_df.write.mode("overwrite").parquet(target_path)

    # Optionally: update metadata table with latest timestamp
    max_tracking_val = full_df.agg({tracking_col: "max"}).collect()[0][0]
    cur.execute("""
        UPDATE cdc_metadata
        SET last_synced_at = %s,
            target_path = %s
        WHERE source_table = %s
    """, (max_tracking_val, target_path, table))
    conn.commit()
    cur.close()
    conn.close()
    print(f"[INFO] Full load completed for {table} — max({tracking_col}) = {max_tracking_val}")
spark.stop()
