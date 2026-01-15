package com.esup.datawarehouse

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Spark Job Application for Data Warehouse
 * This is a template for creating Spark jobs that connect to Hive and Iceberg
 */
object SparkJobApp {

  def main(args: Array[String]): Unit = {
    // Create Spark Session with Hive and Iceberg support
    val spark = SparkSession.builder()
      .appName("dp-spark-job")
      .config("spark.sql.session.timeZone", "Asia/Shanghai")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.catalog.spark_catalog.uri", "thrift://p-bdm-app03:9083")
      .config("spark.sql.catalog.spark_catalog.warehouse", "hdfs://p-bdm-app02:8020/user/hive/warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .enableHiveSupport()
      .getOrCreate()

    try {
      println("=" * 80)
      println("Spark Session Created Successfully")
      println("=" * 80)
      println(s"Spark Version: ${spark.version}")
      println(s"Application Name: ${spark.sparkContext.appName}")
      println(s"Master: ${spark.sparkContext.master}")
      println(s"Deploy Mode: ${spark.sparkContext.deployMode}")
      println("=" * 80)

      // Test Hive connectivity
      println("\n>>> Testing Hive Connectivity...")
      val databases = spark.sql("SHOW DATABASES")
      println("Available databases:")
      databases.show(false)

      // Test HDFS connectivity
      println("\n>>> Testing HDFS Connectivity...")
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)
      val defaultFS = hadoopConf.get("fs.defaultFS")
      println(s"Default FileSystem: $defaultFS")
      println(s"FileSystem Status: ${if (fs != null) "Connected" else "Failed"}")

      // Example: Query from Hive table (if exists)
      println("\n>>> Listing tables in default database...")
      val tables = spark.sql("SHOW TABLES IN default")
      tables.show(false)

      // Example: Create a sample Iceberg table (commented out by default)
      /*
      println("\n>>> Creating sample Iceberg table...")
      spark.sql("""
        CREATE TABLE IF NOT EXISTS default.sample_iceberg_table (
          id BIGINT,
          name STRING,
          created_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(created_at))
      """)
      println("Sample Iceberg table created successfully")
      */

      println("\n" + "=" * 80)
      println("Spark Job Completed Successfully")
      println("=" * 80)

    } catch {
      case e: Exception =>
        println(s"\n!!! Error occurred: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}
