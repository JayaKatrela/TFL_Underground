from pyspark.sql import SparkSession
import psycopg2
import pandas as pd
from datetime import datetime

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

# Step 1: Load CDC metadata from PostgreSQL
cdc_meta_df = spark.read.jdbc(url=jdbc_url, table="cdc_metadata", properties=properties)
active_tables = cdc_meta_df.filter("is_active = true").collect()

for row in active_tables:
    table = row['cdc1']
    pk = row['user_id']
    tracking_col = row['username']
    last_sync = row['created_at']
    target_path = row['s3://cdcimplementation/cdc1/']

    # Step 2: Load changed data from source table
    query = f"(SELECT * FROM cdc1 WHERE {tracking_col} > '{last_sync}') AS t"
    changed_df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

    if changed_df.count() > 0:
        # Step 3: Write changed data to target (e.g., Delta, S3, etc.)
        changed_df.write.mode("append").parquet(target_path)  # Change to delta/parquet/json as needed

        # Step 4: Update last_synced_at in metadata
        new_max = changed_df.agg({tracking_col: "max"}).collect()[0][0]
        conn = psycopg2.connect(host="<host>", dbname="<database>", user="<username>", password="<password>")
        cur = conn.cursor()
        cur.execute("""
            UPDATE cdc_metadata 
            SET last_synced_at = %s 
            WHERE source_table = %s
        """, (new_max, table))
        conn.commit()
        cur.close()
        conn.close()
