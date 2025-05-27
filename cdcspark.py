import os
import psycopg2
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Incremental CDC Load") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-2.amazonaws.com") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://18.170.23.150:5432/testdb"
properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

# Step 1: Read metadata
metadata_df = spark.read.jdbc(url=jdbc_url, table="cdc_metadata", properties=properties)
metadata_rows = metadata_df.collect()

# Step 2: Process each table based on last_synced_at
for idx, row in enumerate(metadata_rows, start=1):
    table = row['source_table']
    tracking_col = row['tracking_column']
    last_synced_at = row['last_synced_at']

    # Step 3: Query for incremental records only
if last_synced_at:
    query = "(SELECT * FROM {} WHERE {} > '{}') AS temp".format(table, tracking_col, last_synced_at)
else:
    query = "(SELECT * FROM {}) AS temp".format(table)



    inc_df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

    if inc_df.count() == 0:
        print("No new data for table {}, skipping...".format(table))
        continue

    # Step 4: Save incremental data to S3
    target_path = "s3a://cdcimplementation1/cdc_{}/{}".format(idx, table)
    inc_df.write.mode("append").parquet(target_path)

    # Step 5: Update last_synced_at
    max_timestamp = inc_df.agg({tracking_col: "max"}).collect()[0][0]

    conn = psycopg2.connect(
        host="18.170.23.150", dbname="testdb",
        user="consultants", password="WelcomeItc@2022"
    )
    cur = conn.cursor()
    cur.execute("""
        UPDATE cdc_metadata
        SET last_synced_at = %s,
            target_path = %s
        WHERE source_table = %s
    """, (max_timestamp, target_path, table))
    conn.commit()
    cur.close()
    conn.close()

spark.stop()
