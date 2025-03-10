package spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object incrload_tfl {
  def main(args: Array[String]): Unit = {

    // Create SparkSession with Hive support
    val spark = SparkSession.builder()
      .appName("Read from Hive")
      .config("spark.sql.catalogImplementation", "hive")  // Enables Hive support
      .enableHiveSupport()  // Required for using Hive
      .getOrCreate()

    // Define JDBC connection parameters
    val jdbcUrl = "jdbc:postgresql://18.170.23.150:5432/testdb"
    val dbTable = "new_tfl2"
    val dbProperties = new java.util.Properties()
    dbProperties.setProperty("user", "consultants")  // Your database username
    dbProperties.setProperty("password", "WelcomeItc@2022")  // Your database password
    dbProperties.setProperty("driver", "org.postgresql.Driver")

    spark.sql(s"USE big_datajan2025")

    val hiveTable = "scala_tfl_underground"
    val maxIdHive = spark.sql(s"SELECT MAX(CAST(uid AS BIGINT)) AS max_id FROM $hiveTable")
      .first()
      .getAs[Long]("max_id")

    println(s"Maximum ID in Hive table: $maxIdHive")

    // Read data from the PostgreSQL table into a DataFrame
    val query = s"(SELECT * FROM new_tfl2 WHERE id > $maxIdHive) AS incremental_data"
    val df_postgres = spark.read
      .jdbc(jdbcUrl, query, dbProperties)  // Replace "your_table_name" with your table name

    val newRowCount = df_postgres.count()
    println(s"Number of new rows to add: $newRowCount")

    println("read successful")

    //df.show(5)

    // Transform the column: Convert String to Timestamp
    val df_transformed = df_postgres.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy/MM/dd HH:mm"))


    // Write DataFrame to Hive table
    df_transformed.write
      .mode("append")  // Use append for adding data without overwriting
      .saveAsTable("big_datajan2025.scala_tfl_underground")  // Specify your database and table name

    println(s"$newRowCount new records added")

    // Stop SparkSession
    spark.stop()
  }
}
