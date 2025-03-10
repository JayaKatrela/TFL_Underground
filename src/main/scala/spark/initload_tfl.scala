package spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object initload_tfl {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("PostgreSQLExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Define JDBC connection parameters
    val jdbcUrl = "jdbc:postgresql://18.170.23.150:5432/testdb"
    val dbProperties = new java.util.Properties()
    dbProperties.setProperty("user", "consultants")  // Your database username
    dbProperties.setProperty("password", "WelcomeItc@2022")  // Your database password
    dbProperties.setProperty("driver", "org.postgresql.Driver")

    // Read data from the PostgreSQL table into a DataFrame
    val df = spark.read
      .jdbc(jdbcUrl, "new_tfl1", dbProperties)  // Replace "your_table_name" with your table name

    println("read successful")



    // Transform the column: Convert String to Timestamp
    val df_transformed = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy/MM/dd HH:mm"))


    // Write DataFrame to Hive table
    df_transformed.write
      .mode("overwrite")  // Use append for adding data without overwriting
      .saveAsTable("big_datajan2025.scala_tfl_underground")  // Specify your database and table name

    // Stop SparkSession
    spark.stop()
  }
}

