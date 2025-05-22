from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("Dynamic CDC Pipeline") \
    .getOrCreate()
# Create a SparkSession
spark = SparkSession.builder \
    .appName("nameof application ") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
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

metadata_df = spark.read.jdbc(url=jdbc_url, table="cdc_metadata", properties=properties)
metadata_rows = metadata_df.collect()

# Step 4: Iterate over each metadata row (source table)
for row in metadata_rows:
    table = row['source_table']
    tracking_col = row['tracking_column']
    
    # Read full table
    query = "(SELECT * FROM {}) AS temp".format(table)
    full_df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
    
    # Define S3 target path
    target_path = "s3a://cdcimplementation/cdc1/{}/".format(table)

    # Write to S3 (overwrite for full load)
    full_df.write.mode("overwrite").parquet(target_path)

    # Optional: Update metadata table with latest timestamp
    max_timestamp = full_df.agg({tracking_col: "max"}).collect()[0][0]

    # Update last_synced_at and target_path in metadata
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

# Stop Spark
spark.stop()
