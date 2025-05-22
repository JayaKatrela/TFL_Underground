
  from pyspark.sql import SparkSession
import psycopg2
spark = SparkSession.builder \
    .appName("Dynamic CDC Pipeline") \
    .getOrCreate()
# Initialize Spark with all needed configs in ONE session
spark = SparkSession.builder \
    .appName("Dynamic CDC Pipeline") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIA3ZWXJNFTVWT5E2VM") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.jars", "/path/to/postgresql-<version>.jar") \
    .getOrCreate()

# PostgreSQL JDBC config
jdbc_url = "jdbc:postgresql://18.170.23.150:5432/testdb"
properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

metadata_df = spark.read.jdbc(url=jdbc_url, table="cdc_metadata", properties=properties)
metadata_rows = metadata_df.collect()

for row in metadata_rows:
    table = row['source_table']
    tracking_col = row['tracking_column']
    
    query = f"(SELECT * FROM {table}) AS temp"
    full_df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
    
    target_path = f"s3a://cdcimplementation/cdc1/{table}/"

    full_df.write.mode("overwrite").parquet(target_path)

    max_timestamp = full_df.agg({tracking_col: "max"}).collect()[0][0]

    # Update metadata in PostgreSQL
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
